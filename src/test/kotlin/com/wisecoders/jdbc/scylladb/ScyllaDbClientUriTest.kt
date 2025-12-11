package com.wisecoders.jdbc.scylladb

import com.wisecoders.common_jdbc.aws.util.AWSUtil
import java.io.IOException
import java.security.GeneralSecurityException
import java.util.Properties
import org.assertj.core.api.WithAssertions
import org.junit.jupiter.api.Test
import org.mockito.MockedStatic
import org.mockito.Mockito
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException

class ScyllaDbClientUriTest : WithAssertions {
    @Test
    fun testUriWithInvalidParameters() {
        assertThatThrownBy { ScyllaDBClientURI("jdbc:cassandra://localhost:9042?name=cassandra", null) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun testSimpleUri() {
        val uri = ScyllaDBClientURI("jdbc:scylladb://localhost:9042", null)
        val hosts: List<String> = uri.hosts
        assertThat(hosts).hasSize(1)
        assertThat(hosts[0]).isEqualTo("localhost:9042")
    }

    @Test
    fun testUriWithUserName() {
        val uri = ScyllaDBClientURI("jdbc:scylladb://localhost:9042/?user=cassandra", null)
        val hosts: List<String> = uri.hosts
        assertThat(hosts).hasSize(1)
        assertThat(hosts[0]).isEqualTo("localhost:9042")
        assertThat(uri.username).isEqualTo("cassandra")
    }

    @Test
    fun testOptionsInProperties() {
        val properties = Properties()
        properties["user"] = "NameFromProperties"
        properties["password"] = "PasswordFromProperties"
        val uri = ScyllaDBClientURI(
            "jdbc:scylladb://localhost:9042/?user=cassandra&password=cassandra",
            properties
        )
        val hosts: List<String> = uri.hosts
        assertThat(hosts).hasSize(1)
        assertThat(hosts[0]).isEqualTo("localhost:9042")
        assertThat(uri.username).isEqualTo("NameFromProperties")
        assertThat(uri.password).isEqualTo("PasswordFromProperties")
    }


    @Test
    @Throws(GeneralSecurityException::class, IOException::class)
    fun testSslEnabledOptionTrue() {
        val properties = Properties()
        properties["sslenabled"] = "true"
        val uri = ScyllaDBClientURI(
            "jdbc:scylladb://localhost:9042/?name=cassandra&password=cassandra",
            properties
        )
        assertThat(uri.sslContext).isNotNull()
        assertThat(uri.sslEnabled).isTrue()
    }

    @Test
    fun testSslEnabledOptionFalse() {
        val properties = Properties()
        properties["sslenabled"] = "false"
        val uri = ScyllaDBClientURI(
            "jdbc:scylladb://localhost:9042/?name=cassandra&password=cassandra",
            properties
        )
        assertThat(uri.sslEnabled).isFalse()
    }

    @Test
    fun testNullSslEnabledOptionFalse() {
        val properties = Properties()
        val uri = ScyllaDBClientURI(
            "jdbc:scylladb://localhost:9042/?name=cassandra&password=cassandra",
            properties
        )
        assertThat(uri.sslEnabled).isFalse()
    }

    @Test
    fun testAwsSecretNotFound() {
        val sme = SecretsManagerException
            .builder()
            .message("Any error")
            .build() as SecretsManagerException

        val region = "sa-east-1"
        val secretName = "a_secret_name"
        val secretKey = "a_secret_key"

        Mockito.mockStatic<AWSUtil>(AWSUtil::class.java).use { utilities ->
            utilities.`when`<Any>(MockedStatic.Verification { AWSUtil.getSecretValue(region, secretName, secretKey) })
                .thenThrow(sme)
            try {
                val uri = ScyllaDBClientURI(
                    "jdbc:scylladb://localhost:9042/?name=cassandra&password=cassandra&awsregion=sa-east-1&awssecretname=a_secret_name&awssecretkey=a_secret_key&anyFieldEndingWithPassword=another_pass",
                    Properties()
                )
            } catch (e: Exception) {
                assertThat<Exception>(e).isInstanceOf(SecretsManagerException::class.java)
            }
        }
    }
}
