package com.wisecoders.jdbc.scylladb

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.instaclustr.cassandra.driver.auth.KerberosAuthProviderBase
import com.instaclustr.cassandra.driver.auth.ProgrammaticKerberosAuthProvider
import com.wisecoders.common_jdbc.aws.util.AWSUtil
import com.wisecoders.common_jdbc.jvm.sql.getSslContext
import com.wisecoders.common_jdbc.jvm.sql.maskAllPasswords
import com.wisecoders.common_jdbc.jvm.sql.parseOptions
import com.wisecoders.common_lib.common_slf4j.slf4jLogger
import java.io.File
import java.io.IOException
import java.net.InetSocketAddress
import java.security.GeneralSecurityException
import java.util.Collections
import java.util.Properties
import javax.net.ssl.SSLContext

/**
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/scylladb-jdbc-driver).
 */
class ScyllaDBClientURI(
    uri: String,
    info: Properties?,
) {
    /**
     * Gets the list of hosts
     *
     * @return the host list
     */
    @JvmField
    var hosts = mutableListOf<String>()

    /**
     * Gets the keyspace name
     *
     * @return the keyspace name
     */
    private var keyspace: String? = null
    private val dataCenter: String?

    /**
     * Gets the collection name
     *
     * @return the collection name
     */
    private var collection: String? = null

    /**
     * Get the unparsed URI.
     *
     * @return the URI
     */
    val uRI: String

    /**
     * Gets the username
     *
     * @return the username
     */
    val username: String?

    /**
     * Gets the password
     *
     * @return the password
     */
    @JvmField
    var password: String? = null

    /**
     * Gets the ssl enabled property
     *
     * @return the ssl enabled property
     */
    val sslEnabled: Boolean
    private val trustStore: String?
    private val trustStorePassword: String?
    private val keyStore: String?
    private val keyStorePassword: String?
    private val configFile: String?
    private val useKerberos: Boolean

    init {
        var uriString = uri
        this.uRI = uriString

        LOGGER.atInfo().setMessage("Use URI: ${uriString.maskAllPasswords()}").log()
        require(uriString.startsWith(PREFIX)) { "URI must start with $PREFIX" }

        uriString = uriString.removePrefix(PREFIX)

        // Split off query/options
        val (mainPart, options) = run {
            val idx = listOf(uriString.indexOf("?"), uriString.indexOf(";"))
                .filter { it > 0 }
                .minOrNull() ?: -1

            if (idx >= 0) {
                val opts = uriString.substring(idx + 1).parseOptions()
                uriString.substring(0, idx) to opts
            } else {
                uriString to emptyMap()
            }
        }

        // Extract server and namespace
        val (serverPart, nsPart) = mainPart.split("/", limit = 2)
            .let { it[0] to it.getOrNull(1) }

        this.username = getOption(info, options, "user")

        // Password or AWS secret
        this.password = run {
            val awsRegion = getOption(info, options, "awsregion")
            val awsSecretName = getOption(info, options, "awssecretname")
            val awsSecretKey = getOption(info, options, "awssecretkey")

            if (awsRegion != null && awsSecretName != null && awsSecretKey != null) {
                LOGGER.atInfo().setMessage("Use AWS password").log()
                AWSUtil.getSecretValue(awsRegion, awsSecretName, awsSecretKey)
            } else {
                getOption(info, options, "password")
            }
        }

        this.dataCenter = getOption(info, options, "dc")
        this.sslEnabled = getOption(info, options, "sslenabled").toBoolean()
        this.useKerberos = getOption(info, options, "kerberos").toBoolean()

        // SSL configs (fall back to system properties)
        this.trustStore = getOption(info, options, "javax.net.ssl.truststore")
            ?: System.getProperty("javax.net.ssl.trustStore")
        this.trustStorePassword = getOption(info, options, "javax.net.ssl.truststorepassword")
            ?: System.getProperty("javax.net.ssl.trustStorePassword")
        this.keyStore = getOption(info, options, "javax.net.ssl.keystore")
            ?: System.getProperty("javax.net.ssl.keyStore")
        this.keyStorePassword = getOption(info, options, "javax.net.ssl.keystorepassword")
            ?: System.getProperty("javax.net.ssl.keyStorePassword")

        this.configFile = getOption(info, options, "configfile")

        // Hosts
        this.hosts = serverPart.split(",")
            .filter { it.isNotBlank() }
            .let { Collections.unmodifiableList(it) }

        // Keyspace / collection
        if (nsPart.isNullOrEmpty()) {
            keyspace = null
            collection = null
        } else {
            val dotIndex = nsPart.indexOf('.')
            if (dotIndex < 0) {
                keyspace = nsPart
                collection = null
            } else {
                keyspace = nsPart.substring(0, dotIndex)
                collection = nsPart.substring(dotIndex + 1)
            }
        }

        LOGGER.atInfo().setMessage(
            "Init hosts=$hosts keyspace=$keyspace collection=$collection " +
                    "user=$username dc=$dataCenter sslenabled=$sslEnabled"
        ).log()
    }


    /**
     * @return option from properties or from uri if it is not found in properties.
     * null if options was not found.
     */
    private fun getOption(
        properties: Properties?,
        options: Map<String, List<String>>,
        optionName: String,
    ): String? {
        if (properties != null) {
            val option = properties[optionName] as String?
            if (option != null) {
                return option
            }
        }
        return getLastValue(options, optionName)
    }

    @Throws(IOException::class, GeneralSecurityException::class)
    fun createCqlSession(): CqlSession {
        val builder = CqlSession.builder()
        var port = 9042
        for (host in hosts) {
            var host = host
            val idx = host.indexOf(":")
            if (idx > 0) {
                port = host.substring(idx + 1).trim { it <= ' ' }.toInt()
                host = host.substring(0, idx).trim { it <= ' ' }
            }
            LOGGER.atInfo().setMessage(("Builder Contact Point: $host:$port")).log()
            builder.addContactPoint(InetSocketAddress(host, port))
            if (sslEnabled) {
                LOGGER.atInfo().setMessage(("Builder SslContext: $sslContext")).log()
                builder.withSslContext(sslContext)
            }
            if (configFile != null) {
                LOGGER.atInfo().setMessage(("Builder Config File: $configFile")).log()
                val file = File(this.configFile)
                builder.withConfigLoader(DriverConfigLoader.fromFile(file))
            }
        }
        LOGGER.atInfo().setMessage(
            "Builder Datacenter: " + (dataCenter
                ?: "datacenter1")
        ).log()
        builder.withLocalDatacenter(dataCenter ?: "datacenter1")
        if (useKerberos) {
            builder.withAuthProvider(
                ProgrammaticKerberosAuthProvider(
                    KerberosAuthProviderBase.KerberosAuthOptions.builder().build()
                )
            )
        }
        if (keyspace != null) {
            LOGGER.atInfo().setMessage(("Builder Keyspace: $keyspace")).log()
            builder.withKeyspace(keyspace)
        }
        val _password = password
        if (!username.isNullOrEmpty() && _password != null) {
            LOGGER.atInfo().setMessage(("Builder Authentication user: $username")).log()
            builder.withAuthCredentials(username, _password)
        }
        return builder.build()
    }


    private fun getLastValue(
        optionsMap: Map<String, List<String>>,
        key: String,
    ): String? {
        val valueList: List<String?>? = optionsMap[key]
        if (valueList.isNullOrEmpty()) return null
        return valueList[valueList.size - 1]
    }


    @get:Throws(GeneralSecurityException::class, IOException::class)
    val sslContext: SSLContext
        get() {
            val trustStore = this.trustStore
            val trustStorePassword = this.trustStorePassword
            val keyStore = this.keyStore
            val keyStorePassword = this.keyStorePassword

            return getSslContext(trustStore, trustStorePassword, keyStore, keyStorePassword)
        }


    override fun toString(): String {
        return uRI
    }

    companion object {
        private val LOGGER = slf4jLogger()

        const val PREFIX: String = "jdbc:scylladb://"
    }
}
