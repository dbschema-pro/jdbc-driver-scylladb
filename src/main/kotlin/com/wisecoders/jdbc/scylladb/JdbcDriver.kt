package com.wisecoders.jdbc.scylladb

import com.wisecoders.common_lib.common_slf4j.slf4jLogger
import java.io.IOException
import java.net.UnknownHostException
import java.security.GeneralSecurityException
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.DriverPropertyInfo
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.util.Properties
import java.util.logging.Logger

/**
 * Minimal implementation of the JDBC standards for the Cassandra database.
 * This is customized for DbSchema database designer.
 * Connect to the database using a URL like :
 * jdbc:cassandra://host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[keyspace][?options]]
 * The URL excepting the jdbc: prefix is passed as it is to the Cassandra native Java driver.
 *
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/scylladb-jdbc-driver).
 */
class JdbcDriver : Driver {
    /**
     * Connect to the database using a URL like :
     * jdbc:cassandra://host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[keyspace][?options]]
     * The URL excepting the jdbc: prefix is passed as it is to the Cassandra native Java driver.
     */
    @Throws(SQLException::class)
    override fun connect(
        url: String?,
        info: Properties,
    ): Connection? {
        if (url != null && acceptsURL(url)) {
            val clientURI = ScyllaDBClientURI(url, info)
            try {
                val session = clientURI.createCqlSession()
                try {
                    session.execute("SELECT cql_version FROM system.local")
                } catch (e: Throwable) {
                    throw SQLException(e.message, e)
                }
                val returnNullStringsFromIntroQuery =
                    info.getProperty(RETURN_NULL_STRINGS_FROM_INTRO_QUERY_KEY).toBoolean()
                return ScyllaDBConnection(session, this, returnNullStringsFromIntroQuery)
            } catch (e: UnknownHostException) {
                throw SQLException(e.message, e)
            } catch (e: GeneralSecurityException) {
                throw SQLException(e.message, e)
            } catch (e: IOException) {
                throw SQLException(e.message, e)
            }
        }
        return null
    }


    /**
     * URLs accepted are of the form: jdbc:cassandra://host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[keyspace][?options]]
     */
    override fun acceptsURL(url: String): Boolean {
        return url.startsWith(ScyllaDBClientURI.PREFIX)
    }

    override fun getPropertyInfo(
        url: String,
        info: Properties,
    ): Array<DriverPropertyInfo>? {
        return null
    }

    val version: String
        get() = "1.3.4"

    override fun getMajorVersion(): Int {
        return 1
    }

    override fun getMinorVersion(): Int {
        return 3
    }

    override fun jdbcCompliant(): Boolean {
        return true
    }

    @Throws(SQLFeatureNotSupportedException::class)
    override fun getParentLogger(): Logger {
        throw SQLFeatureNotSupportedException()
    }

    companion object {
        private const val RETURN_NULL_STRINGS_FROM_INTRO_QUERY_KEY =
            "cassandra.jdbc.return.null.strings.from.intro.query"

        private val LOGGER = slf4jLogger()

        init {
            try {
                DriverManager.registerDriver(JdbcDriver())
            } catch (ex: Exception) {
                ex.printStackTrace()
            }
        }


    }

}
