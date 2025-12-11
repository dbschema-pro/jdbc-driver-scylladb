package com.wisecoders.jdbc.scylladb

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder
import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.oss.driver.api.core.servererrors.SyntaxError
import java.sql.Connection
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLSyntaxErrorException
import java.sql.SQLWarning
import java.sql.Statement

/**
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/scylladb-jdbc-driver).
 */
abstract class ScyllaDBBaseStatement internal constructor(@JvmField val session: CqlSession) : Statement {
    @JvmField
    var batchStatementBuilder: BatchStatementBuilder? = null
    private var isClosed = false
    @JvmField
    var result: ScyllaDBResultSet? = null

    override fun close() {
        isClosed = true
    }

    @Throws(SQLException::class)
    fun checkClosed() {
        if (isClosed) {
            throw SQLException("Statement was previously closed.")
        }
    }

    override fun isClosed(): Boolean {
        return isClosed
    }

    @Throws(SQLException::class)
    fun executeInner(
        resultSet: ResultSet,
        returnNullStrings: Boolean
    ): Boolean {
        try {
            result = ScyllaDBResultSet(this, resultSet, returnNullStrings)
            if (!result!!.isQuery) {
                result = null
                return false
            }
            return true
        } catch (ex: SyntaxError) {
            throw SQLSyntaxErrorException(ex.message, ex)
        } catch (t: Throwable) {
            throw SQLException(t.message, t)
        }
    }

    override fun getMoreResults(): Boolean {
        // todo
        return false
    }

    @Throws(SQLException::class)
    override fun getUpdateCount(): Int {
        checkClosed()
        return -1
    }

    @Throws(SQLException::class)
    override fun executeBatch(): IntArray {
        if (batchStatementBuilder == null) throw SQLException("No batch statements were submitted")
        val statementsCount = batchStatementBuilder!!.statementsCount
        try {
            session.execute(batchStatementBuilder!!.build())
        } catch (t: Throwable) {
            throw SQLException(t.message, t)
        } finally {
            batchStatementBuilder = null
        }
        val res = IntArray(statementsCount)
        for (i in 0..<statementsCount) {
            res[i] = Statement.SUCCESS_NO_INFO
        }
        return res
    }

    @Throws(SQLException::class)
    override fun <T> unwrap(iface: Class<T>): T {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun isWrapperFor(iface: Class<*>?): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getResultSetHoldability(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setPoolable(poolable: Boolean) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun isPoolable(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun closeOnCompletion() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun isCloseOnCompletion(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxFieldSize(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setMaxFieldSize(max: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxRows(): Int {
        throw SQLFeatureNotSupportedException()
    }

    override fun setMaxRows(max: Int) {
        // todo
    }

    @Throws(SQLException::class)
    override fun setEscapeProcessing(enable: Boolean) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getQueryTimeout(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun setQueryTimeout(seconds: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun cancel() {
        throw SQLFeatureNotSupportedException("Cassandra provides no support for interrupting an operation.")
    }

    override fun getWarnings(): SQLWarning? {
        return null // todo
    }

    override fun clearWarnings() {
        // todo
    }

    @Throws(SQLException::class)
    override fun setCursorName(name: String) {
        checkClosed()
        // Driver doesn't support positioned updates for now, so no-op.
    }

    @Throws(SQLException::class)
    override fun setFetchDirection(direction: Int) {
        throw SQLFeatureNotSupportedException()
    }

    override fun getFetchDirection(): Int {
        return java.sql.ResultSet.FETCH_FORWARD
    }

    override fun setFetchSize(rows: Int) {
        // todo
    }

    @Throws(SQLException::class)
    override fun getFetchSize(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getResultSetConcurrency(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getResultSetType(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getConnection(): Connection {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMoreResults(current: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getGeneratedKeys(): java.sql.ResultSet {
        throw SQLFeatureNotSupportedException()
    }
}
