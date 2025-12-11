package com.wisecoders.jdbc.scylladb

import com.wisecoders.common_jdbc.jvm.sql.printResultSet
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

/**
 * This test works with a local docker container.
 */
@Disabled("disabled until we figure out how to run tests needing docker containers")
class TestMetadata {
    private var con: Connection? = null

    // THIS TESTS ARE USING A LOCAL INSTALLED CASSANDRA (I USE DOCKER)
    @BeforeEach
    @Throws(ClassNotFoundException::class, SQLException::class)
    fun setUp() {
        Class.forName("com.wisecoders.jdbc.scylladb.JdbcDriver")
        val _con = DriverManager.getConnection(URL_WITHOUT_AUTH, "cassandra", "cassandra")
        this.con = _con
        val stmt = _con.createStatement()
        stmt.execute("SELECT cql_version FROM system.local")
        stmt.executeQuery("CREATE KEYSPACE IF NOT EXISTS dbschema WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 3 } AND DURABLE_WRITES = false")
        stmt.executeQuery("CREATE TABLE IF NOT EXISTS dbschema.cyclist_category ( category text, points int, id UUID, lastname text, PRIMARY KEY (category, points)) WITH CLUSTERING ORDER BY (points DESC)")
        stmt.executeQuery("CREATE TABLE IF NOT EXISTS dbschema.rank_by_year_and_name ( race_year int, race_name text,  cyclist_name text, rank int,  PRIMARY KEY ((race_year, race_name), rank) )")
        stmt.executeQuery("CREATE TYPE IF NOT EXISTS dbschema.basic_info ( birthday timestamp,  nationality text, weight text, height text)")
        stmt.executeQuery("CREATE TABLE IF NOT EXISTS dbschema.cyclist_stats ( id uuid PRIMARY KEY, lastname text, basics FROZEN<basic_info>) ")
        stmt.close()
    }


    @Test
    @Throws(Exception::class)
    fun testMetaData() {
        con!!.metaData.catalogs.printResultSet()
        con!!.metaData.getTables("dbschema", null, null, null).printResultSet()
    }

    @Test
    @Throws(Exception::class)
    fun testReadTimestamp() {
        val stmt = con!!.createStatement()
        val rs = stmt.executeQuery("SELECT id, lastname, basics FROM dbschema.cyclist_stats")
        rs.printResultSet()
        stmt.close()
    }

    @Test
    @Throws(Exception::class)
    fun testFind() {
        val stmt = con!!.createStatement()
        val rs = stmt.executeQuery("SELECT cql_version FROM system.local")
        rs.printResultSet()
        stmt.close()
    }

    @Test
    @Throws(Exception::class)
    fun testDescribe() {
        val stmt = con!!.createStatement()
        stmt.executeQuery("DESC dbschema").printResultSet()
        stmt.executeQuery("DESC dbschema.cyclist_category").printResultSet()
        stmt.close()
    }

    companion object {
        private const val URL_WITHOUT_AUTH = "jdbc:scylladb://localhost/system?expand=true"

    }
}
