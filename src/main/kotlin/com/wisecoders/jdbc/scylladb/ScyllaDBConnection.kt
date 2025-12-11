package com.wisecoders.jdbc.scylladb

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.oss.driver.api.core.servererrors.SyntaxError
import com.wisecoders.common_jdbc.jvm.result_set.ArrayResultSet
import java.sql.Blob
import java.sql.CallableStatement
import java.sql.Clob
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.NClob
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLSyntaxErrorException
import java.sql.SQLWarning
import java.sql.SQLXML
import java.sql.Savepoint
import java.sql.Statement
import java.sql.Struct
import java.util.Properties
import java.util.concurrent.Executor
import java.util.regex.Pattern


/**
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/scylladb-jdbc-driver).
 */
class ScyllaDBConnection internal constructor(
    @JvmField val session: CqlSession,
    private val driver: JdbcDriver,
    private val returnNullStringsFromIntroQuery: Boolean
) :
    Connection {
    private var isClosed = false
    private var isReadOnly = false

    @Throws(SQLException::class)
    override fun getCatalog(): String {
        checkClosed()
        try {
            return session.keyspace.toString()
        } catch (t: Throwable) {
            throw SQLException(t.message, t)
        }
    }

    fun executeDescribeCommand(sql: String): ResultSet? {
        val rs = ArrayResultSet(listOf("KEYSPACE", "CAT", "OBJECT", "DESC"))
        run {
            val matcher = describeTable.matcher(sql)
            if (matcher.matches()) {
                session.metadata.getKeyspace(matcher.group(1)).ifPresent { keyspaceMetadata: KeyspaceMetadata ->
                    keyspaceMetadata.getTable(matcher.group(2))
                        .ifPresent { tableMetadata: TableMetadata ->
                            rs.addRow(
                                listOf(
                                    keyspaceMetadata.name.toString(),
                                    null,
                                    matcher.group(1),
                                    tableMetadata.describeWithChildren(true)
                                )
                            )
                        }
                }
                return rs
            }
        }
        run {
            val matcher = describeKeyspace.matcher(sql)
            if (matcher.matches()) {
                session.metadata.getKeyspace(matcher.group(1)).ifPresent { keyspaceMetadata: KeyspaceMetadata ->
                    rs.addRow(
                        listOf(
                            keyspaceMetadata.name.toString(),
                            null,
                            matcher.group(1),
                            keyspaceMetadata.describeWithChildren(true)
                        )
                    )
                }
                return rs
            }
        }
        return null
    }

    @Throws(SQLException::class)
    override fun <T> unwrap(iface: Class<T>): T? {
        checkClosed()
        return null
    }

    @Throws(SQLException::class)
    override fun isWrapperFor(iface: Class<*>?): Boolean {
        checkClosed()
        return false
    }

    @Throws(SQLException::class)
    override fun createStatement(): Statement {
        checkClosed()
        try {
            return ScyllaDBStatement(this)
        } catch (t: Throwable) {
            throw SQLException(t.message, t)
        }
    }

    @Throws(SQLException::class)
    override fun createStatement(
        resultSetType: Int,
        resultSetConcurrency: Int
    ): Statement {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun createStatement(
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): Statement {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun prepareStatement(sql: String): PreparedStatement {
        checkClosed()
        try {
            return ScyllaDBPreparedStatement(
                session,
                session.prepare(sql),
                returnNullStringsFromIntroQuery || SELECT_COLUMNS_INTRO_QUERY != sql
            )
        } catch (error: SyntaxError) {
            throw SQLSyntaxErrorException(error.message, error)
        } catch (t: Throwable) {
            throw SQLException(t.message, t)
        }
    }

    @Throws(SQLException::class)
    override fun prepareCall(sql: String): CallableStatement {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun nativeSQL(sql: String): String {
        checkClosed()
        throw SQLFeatureNotSupportedException("Cassandra does not support SQL natively.")
    }

    @Throws(SQLException::class)
    override fun setAutoCommit(autoCommit: Boolean) {
        checkClosed()
    }

    @Throws(SQLException::class)
    override fun getAutoCommit(): Boolean {
        checkClosed()
        return true
    }

    @Throws(SQLException::class)
    override fun commit() {
        checkClosed()
    }

    @Throws(SQLException::class)
    override fun rollback() {
        checkClosed()
    }

    override fun close() {
        isClosed = true
    }

    override fun isClosed(): Boolean {
        return isClosed
    }

    @Throws(SQLException::class)
    override fun getMetaData(): DatabaseMetaData {
        checkClosed()
        return ScyllaDBMetaData(this, driver)
    }

    @Throws(SQLException::class)
    override fun setReadOnly(readOnly: Boolean) {
        checkClosed()
        isReadOnly = readOnly
    }

    @Throws(SQLException::class)
    override fun isReadOnly(): Boolean {
        checkClosed()
        return isReadOnly
    }

    override fun setCatalog(catalog: String) {
    }

    @Throws(SQLException::class)
    override fun setTransactionIsolation(level: Int) {
        checkClosed()
        // Since the only valid value for MongDB is Connection.TRANSACTION_NONE, and the javadoc for this method
        // indicates that this is not a valid value for level here, throw unsupported operation exception.
        throw UnsupportedOperationException("Cassandra provides no support for transactions.")
    }

    @Throws(SQLException::class)
    override fun getTransactionIsolation(): Int {
        checkClosed()
        return Connection.TRANSACTION_NONE
    }

    @Throws(SQLException::class)
    override fun getWarnings(): SQLWarning? {
        checkClosed()
        return null
    }

    @Throws(SQLException::class)
    override fun clearWarnings() {
        checkClosed()
    }


    @Throws(SQLException::class)
    override fun prepareStatement(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int
    ): PreparedStatement {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun prepareCall(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int
    ): CallableStatement {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getTypeMap(): Map<String, Class<*>> {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setTypeMap(map: Map<String?, Class<*>?>?) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setHoldability(holdability: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getHoldability(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setSavepoint(): Savepoint {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setSavepoint(name: String): Savepoint {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun rollback(savepoint: Savepoint) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun releaseSavepoint(savepoint: Savepoint) {
        throw SQLFeatureNotSupportedException()
    }


    @Throws(SQLException::class)
    override fun prepareStatement(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): PreparedStatement {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun prepareCall(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): CallableStatement {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun prepareStatement(
        sql: String,
        autoGeneratedKeys: Int
    ): PreparedStatement {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun prepareStatement(
        sql: String,
        columnIndexes: IntArray
    ): PreparedStatement {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun prepareStatement(
        sql: String,
        columnNames: Array<String>
    ): PreparedStatement {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun createClob(): Clob {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun createBlob(): Blob {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun createNClob(): NClob? {
        checkClosed()
        return null
    }

    @Throws(SQLException::class)
    override fun createSQLXML(): SQLXML? {
        checkClosed()
        return null
    }

    @Throws(SQLException::class)
    override fun isValid(timeout: Int): Boolean {
        checkClosed()
        return true
    }

    override fun setClientInfo(
        name: String,
        value: String
    ) {
        /* Cassandra does not support setting client information in the database. */
    }

    override fun setClientInfo(properties: Properties) {
        /* Cassandra does not support setting client information in the database. */
    }

    @Throws(SQLException::class)
    override fun getClientInfo(name: String): String? {
        checkClosed()
        return null
    }

    @Throws(SQLException::class)
    override fun getClientInfo(): Properties? {
        checkClosed()
        return null
    }

    @Throws(SQLException::class)
    override fun createArrayOf(
        typeName: String,
        elements: Array<Any>
    ): java.sql.Array? {
        checkClosed()

        return null
    }

    @Throws(SQLException::class)
    override fun createStruct(
        typeName: String,
        attributes: Array<Any>
    ): Struct? {
        checkClosed()
        return null
    }


    @Throws(SQLException::class)
    private fun checkClosed() {
        if (isClosed) {
            throw SQLException("Statement was previously closed.")
        }
    }

    override fun setSchema(schema: String) {
        catalog = schema
    }

    @Throws(SQLException::class)
    override fun getSchema(): String {
        return catalog
    }

    @Throws(SQLException::class)
    override fun abort(executor: Executor) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setNetworkTimeout(
        executor: Executor,
        milliseconds: Int
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getNetworkTimeout(): Int {
        throw SQLFeatureNotSupportedException()
    }


    companion object {
        private val describeTable: Pattern = Pattern.compile("DESC (.*)\\.(.*)", Pattern.CASE_INSENSITIVE)
        private val describeKeyspace: Pattern = Pattern.compile("DESC (.*)", Pattern.CASE_INSENSITIVE)


        private const val SELECT_COLUMNS_INTRO_QUERY =
            "SELECT column_name as name,\n       validator,\n       columnfamily_name as table_name,\n       type,\n       index_name,\n       index_options,\n       index_type,\n       component_index as position\nFROM system.schema_columns\nWHERE keyspace_name = ?"
    }
}
