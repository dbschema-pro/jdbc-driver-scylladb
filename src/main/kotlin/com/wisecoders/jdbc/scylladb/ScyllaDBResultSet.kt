package com.wisecoders.jdbc.scylladb

import com.datastax.oss.driver.api.core.cql.ColumnDefinition
import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.oss.driver.api.core.cql.Row
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream
import com.wisecoders.common_jdbc.jvm.result_set.SqlArrayImpl
import com.wisecoders.common_jdbc.jvm.sql.BlobImpl
import com.wisecoders.jdbc.scylladb.DateUtil.considerTimeZone
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.nio.charset.StandardCharsets
import java.sql.Array
import java.sql.Blob
import java.sql.Clob
import java.sql.Date
import java.sql.NClob
import java.sql.Ref
import java.sql.ResultSetMetaData
import java.sql.RowId
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLWarning
import java.sql.SQLXML
import java.sql.Statement
import java.sql.Time
import java.sql.Timestamp
import java.util.Calendar

/**
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/scylladb-jdbc-driver).
 */
class ScyllaDBResultSet @JvmOverloads internal constructor(
    private val statement: Statement,
    private val dsResultSet: ResultSet,
    private val returnNullStrings: Boolean = true
) :
    java.sql.ResultSet {
    private var isClosed = false

    private val iterator: Iterator<Row> = dsResultSet.iterator()
    private var currentRow: Row? = null

    @Throws(SQLException::class)
    override fun <T> unwrap(iface: Class<T>): T {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun isWrapperFor(iface: Class<*>?): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    override fun next(): Boolean {
        if (iterator.hasNext()) {
            currentRow = iterator.next()
            return true
        }
        return false
    }

    override fun close() {
        isClosed = true
    }

    val isQuery: Boolean
        get() = dsResultSet.columnDefinitions.size() != 0

    override fun wasNull(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun getString(columnIndex: Int): String? {
        checkClosed()
        if (currentRow == null) throw SQLException("Exhausted ResultSet.")
        val o = currentRow!!.getObject(columnIndex - 1)
        return o?.toString() ?: if (returnNullStrings) null else ""
    }

    @Throws(SQLException::class)
    override fun getBoolean(columnIndex: Int): Boolean {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getBoolean(columnIndex - 1)
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getByte(columnIndex: Int): Byte {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getByte(columnIndex - 1)
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getShort(columnIndex: Int): Short {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getShort(columnIndex - 1)
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getInt(columnIndex: Int): Int {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getInt(columnIndex - 1)
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getLong(columnIndex: Int): Long {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getLong(columnIndex - 1)
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getFloat(columnIndex: Int): Float {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getFloat(columnIndex - 1)
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getDouble(columnIndex: Int): Double {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getDouble(columnIndex - 1)
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Deprecated("Deprecated in Java")
    @Throws(SQLException::class)
    override fun getBigDecimal(
        columnIndex: Int,
        scale: Int
    ): BigDecimal? {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getBigDecimal(columnIndex - 1)
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getBytes(columnIndex: Int): ByteArray? {
        checkClosed()
        if (currentRow != null) {
            val bytes = currentRow!!.getByteBuffer(columnIndex - 1)
            return bytes?.array()
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getDate(columnIndex: Int): Date? {
        checkClosed()
        if (currentRow != null) {
            val date = currentRow!!.getLocalDate(columnIndex - 1)
            return if (date != null) Date.valueOf(date) else null
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getTime(columnIndex: Int): Time {
        checkClosed()
        if (currentRow != null) {
            val time = currentRow!!.getLocalTime(columnIndex - 1)
            return Time.valueOf(time)
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getTimestamp(columnIndex: Int): Timestamp? {
        checkClosed()
        if (currentRow != null) {
            val instant = currentRow!!.getInstant(columnIndex - 1)
            return if (instant != null) Timestamp.from(instant) else null
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getAsciiStream(columnIndex: Int): InputStream {
        checkClosed()
        if (currentRow != null) {
            return ByteArrayInputStream(currentRow!!.getString(columnIndex)!!.toByteArray(StandardCharsets.UTF_8))
        }
        throw SQLException("Result exhausted.")
    }

    @Deprecated("Deprecated in Java")
    @Throws(SQLException::class)
    override fun getUnicodeStream(columnIndex: Int): InputStream {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getBinaryStream(columnIndex: Int): InputStream {
        checkClosed()
        if (currentRow != null) {
            return ByteBufferBackedInputStream(currentRow!!.getByteBuffer(columnIndex))
        }
        throw SQLException("Result exhausted.")
    }

    @Throws(SQLException::class)
    override fun getString(columnLabel: String): String? {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getString(columnLabel)
        }
        throw SQLException("Result exhausted.")
    }

    @Throws(SQLException::class)
    override fun getBoolean(columnLabel: String): Boolean {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getBoolean(columnLabel)
        }
        throw SQLException("Result exhausted.")
    }

    @Throws(SQLException::class)
    override fun getByte(columnLabel: String): Byte {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getByte(columnLabel)
        }
        throw SQLException("Result exhausted.")
    }

    @Throws(SQLException::class)
    override fun getShort(columnLabel: String): Short {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getShort(columnLabel)
        }
        throw SQLException("Result exhausted.")
    }

    @Throws(SQLException::class)
    override fun getInt(columnLabel: String): Int {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getInt(columnLabel)
        }
        throw SQLException("Result exhausted.")
    }

    @Throws(SQLException::class)
    override fun getLong(columnLabel: String): Long {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getLong(columnLabel)
        }
        throw SQLException("Result exhausted.")
    }

    @Throws(SQLException::class)
    override fun getFloat(columnLabel: String): Float {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getFloat(columnLabel)
        }
        throw SQLException("Result exhausted.")
    }

    @Throws(SQLException::class)
    override fun getDouble(columnLabel: String): Double {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getDouble(columnLabel)
        }
        throw SQLException("Result exhausted.")
    }

    @Deprecated("Deprecated in Java")
    @Throws(SQLException::class)
    override fun getBigDecimal(
        columnLabel: String,
        scale: Int
    ): BigDecimal {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getBytes(columnLabel: String): ByteArray? {
        checkClosed()
        if (currentRow != null) {
            val bytes = currentRow!!.getByteBuffer(columnLabel)
            return bytes?.array()
        }
        throw SQLException("Result exhausted.")
    }

    @Throws(SQLException::class)
    override fun getDate(columnLabel: String): Date {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getTime(columnLabel: String): Time {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getTimestamp(columnLabel: String): Timestamp {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getAsciiStream(columnLabel: String): InputStream {
        throw SQLFeatureNotSupportedException()
    }

    @Deprecated("Deprecated in Java")
    @Throws(SQLException::class)
    override fun getUnicodeStream(columnLabel: String): InputStream {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getBinaryStream(columnLabel: String): InputStream {
        throw SQLFeatureNotSupportedException()
    }

    override fun getWarnings(): SQLWarning? {
        // SUGGESTED BY CRISTI TO SHOW EXECUTION WARNINGS
        val sb = StringBuilder()
        for (warning in dsResultSet.executionInfo.warnings) {
            sb.append(warning).append(" ")
        }
        return if (sb.isNotEmpty()) SQLWarning(sb.toString()) else null
    }

    override fun clearWarnings() {
        // todo
    }

    @Throws(SQLException::class)
    override fun getCursorName(): String {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMetaData(): ResultSetMetaData {
        checkClosed()
        dsResultSet.columnDefinitions
        val columnMetaData: MutableList<ScyllaDBResultSetMetaData.ColumnMetaData> = ArrayList()
        val itr: Iterator<ColumnDefinition> = dsResultSet.columnDefinitions.iterator()
        while (itr.hasNext()) {
            val def = itr.next()
            columnMetaData.add(
                ScyllaDBResultSetMetaData.ColumnMetaData(
                    def.name.toString(),
                    def.table.toString(),
                    def.keyspace.toString(),
                    def.type.toString()
                )
            )
        }
        return ScyllaDBResultSetMetaData(columnMetaData)
    }


    @Throws(SQLException::class)
    override fun getObject(columnIndex: Int): Any? {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getObject(columnIndex - 1)
        }
        throw SQLException("Result exhausted.")
    }

    @Throws(SQLException::class)
    override fun getObject(columnLabel: String): Any? {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getObject(columnLabel)
        }
        throw SQLException("Result exhausted.")
    }

    override fun findColumn(columnLabel: String): Int {
        return dsResultSet.columnDefinitions.firstIndexOf(columnLabel)
    }

    @Throws(SQLException::class)
    override fun getCharacterStream(columnIndex: Int): Reader {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getCharacterStream(columnLabel: String): Reader {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getBigDecimal(columnIndex: Int): BigDecimal? {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getBigDecimal(columnIndex - 1)
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getBigDecimal(columnLabel: String): BigDecimal? {
        checkClosed()
        if (currentRow != null) {
            return currentRow!!.getBigDecimal(columnLabel)
        }
        throw SQLException("Result exhausted.")
    }

    @Throws(SQLException::class)
    override fun isBeforeFirst(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun isAfterLast(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun isFirst(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun isLast(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun beforeFirst() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun afterLast() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun first(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun last(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getRow(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun absolute(row: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun relative(rows: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun previous(): Boolean {
        throw SQLFeatureNotSupportedException()
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

    override fun getFetchSize(): Int {
        return 0
    }

    override fun getType(): Int {
        return java.sql.ResultSet.TYPE_FORWARD_ONLY // todo
    }

    @Throws(SQLException::class)
    override fun getConcurrency(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun rowUpdated(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun rowInserted(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun rowDeleted(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNull(columnIndex: Int) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBoolean(
        columnIndex: Int,
        x: Boolean
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateByte(
        columnIndex: Int,
        x: Byte
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateShort(
        columnIndex: Int,
        x: Short
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateInt(
        columnIndex: Int,
        x: Int
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateLong(
        columnIndex: Int,
        x: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateFloat(
        columnIndex: Int,
        x: Float
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateDouble(
        columnIndex: Int,
        x: Double
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBigDecimal(
        columnIndex: Int,
        x: BigDecimal
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateString(
        columnIndex: Int,
        x: String
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBytes(
        columnIndex: Int,
        x: ByteArray
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateDate(
        columnIndex: Int,
        x: Date
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateTime(
        columnIndex: Int,
        x: Time
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateTimestamp(
        columnIndex: Int,
        x: Timestamp
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(
        columnIndex: Int,
        x: InputStream,
        length: Int
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(
        columnIndex: Int,
        x: InputStream,
        length: Int
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(
        columnIndex: Int,
        x: Reader,
        length: Int
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateObject(
        columnIndex: Int,
        x: Any,
        scaleOrLength: Int
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateObject(
        columnIndex: Int,
        x: Any
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNull(columnLabel: String) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBoolean(
        columnLabel: String,
        x: Boolean
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateByte(
        columnLabel: String,
        x: Byte
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateShort(
        columnLabel: String,
        x: Short
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateInt(
        columnLabel: String,
        x: Int
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateLong(
        columnLabel: String,
        x: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateFloat(
        columnLabel: String,
        x: Float
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateDouble(
        columnLabel: String,
        x: Double
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBigDecimal(
        columnLabel: String,
        x: BigDecimal
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateString(
        columnLabel: String,
        x: String
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBytes(
        columnLabel: String,
        x: ByteArray
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateDate(
        columnLabel: String,
        x: Date
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateTime(
        columnLabel: String,
        x: Time
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateTimestamp(
        columnLabel: String,
        x: Timestamp
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(
        columnLabel: String,
        x: InputStream,
        length: Int
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(
        columnLabel: String,
        x: InputStream,
        length: Int
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(
        columnLabel: String,
        reader: Reader,
        length: Int
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateObject(
        columnLabel: String,
        x: Any,
        scaleOrLength: Int
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateObject(
        columnLabel: String,
        x: Any
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun insertRow() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateRow() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun deleteRow() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun refreshRow() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun cancelRowUpdates() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun moveToInsertRow() {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun moveToCurrentRow() {
        throw SQLFeatureNotSupportedException()
    }

    override fun getStatement(): Statement {
        return this.statement
    }

    @Throws(SQLException::class)
    override fun getObject(
        columnIndex: Int,
        map: Map<String?, Class<*>?>?
    ): Any {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getRef(columnIndex: Int): Ref {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getBlob(columnIndex: Int): Blob? {
        checkClosed()
        if (currentRow != null) {
            val bytes = currentRow!!.getByteBuffer(columnIndex - 1)
            return if (bytes == null) null else BlobImpl(bytes.array())
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getClob(columnIndex: Int): Clob {
        throw SQLException("Clob type is not supported by Cassandra")
    }

    @Throws(SQLException::class)
    override fun getArray(columnIndex: Int): Array? {
        checkClosed()
        if (currentRow != null) {
            val o = currentRow!!.getObject(columnIndex - 1) as? List<*> ?: return null
            return SqlArrayImpl( arrayOf(o) )
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getObject(
        columnLabel: String,
        map: Map<String?, Class<*>?>?
    ): Any {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getRef(columnLabel: String): Ref {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getBlob(columnLabel: String): Blob? {
        checkClosed()
        if (currentRow != null) {
            val bytes = currentRow!!.getByteBuffer(columnLabel)
            return if (bytes == null) null else BlobImpl(bytes.array())
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getClob(columnLabel: String): Clob {
        throw SQLException("Clob type is not supported by Cassandra")
    }

    @Throws(SQLException::class)
    override fun getArray(columnLabel: String): Array? {
        checkClosed()
        if (currentRow != null) {
            val o = currentRow!!.getObject(columnLabel) as? List<*> ?: return null
            return SqlArrayImpl( arrayOf(o))
        }
        throw SQLException("Exhausted ResultSet.")
    }

    @Throws(SQLException::class)
    override fun getDate(
        columnIndex: Int,
        cal: Calendar
    ): Date {
        return considerTimeZone(getDate(columnIndex)!!, cal, DateUtil.Direction.FROM_UTC)
    }

    @Throws(SQLException::class)
    override fun getTime(
        columnIndex: Int,
        cal: Calendar
    ): Time {
        return considerTimeZone(getTime(columnIndex), cal, DateUtil.Direction.FROM_UTC)
    }

    @Throws(SQLException::class)
    override fun getTimestamp(
        columnIndex: Int,
        cal: Calendar
    ): Timestamp {
        return considerTimeZone(getTimestamp(columnIndex)!!, cal, DateUtil.Direction.FROM_UTC)
    }

    @Throws(SQLException::class)
    override fun getDate(
        columnLabel: String,
        cal: Calendar
    ): Date {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getTime(
        columnLabel: String,
        cal: Calendar
    ): Time {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getTimestamp(
        columnLabel: String,
        cal: Calendar
    ): Timestamp {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getURL(columnIndex: Int): URL {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getURL(columnLabel: String): URL {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateRef(
        columnIndex: Int,
        x: Ref
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateRef(
        columnLabel: String,
        x: Ref
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBlob(
        columnIndex: Int,
        x: Blob
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBlob(
        columnLabel: String,
        x: Blob
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateClob(
        columnIndex: Int,
        x: Clob
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateClob(
        columnLabel: String,
        x: Clob
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateArray(
        columnIndex: Int,
        x: Array
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateArray(
        columnLabel: String,
        x: Array
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getRowId(columnIndex: Int): RowId {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getRowId(columnLabel: String): RowId {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateRowId(
        columnIndex: Int,
        x: RowId
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateRowId(
        columnLabel: String,
        x: RowId
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getHoldability(): Int {
        throw SQLFeatureNotSupportedException()
    }

    override fun isClosed(): Boolean {
        return isClosed
    }

    @Throws(SQLException::class)
    override fun updateNString(
        columnIndex: Int,
        nString: String
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNString(
        columnLabel: String,
        nString: String
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNClob(
        columnIndex: Int,
        nClob: NClob
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNClob(
        columnLabel: String,
        nClob: NClob
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getNClob(columnIndex: Int): NClob {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getNClob(columnLabel: String): NClob {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getSQLXML(columnIndex: Int): SQLXML {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getSQLXML(columnLabel: String): SQLXML {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateSQLXML(
        columnIndex: Int,
        xmlObject: SQLXML
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateSQLXML(
        columnLabel: String,
        xmlObject: SQLXML
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getNString(columnIndex: Int): String {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getNString(columnLabel: String): String {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getNCharacterStream(columnIndex: Int): Reader {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getNCharacterStream(columnLabel: String): Reader {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNCharacterStream(
        columnIndex: Int,
        x: Reader,
        length: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNCharacterStream(
        columnLabel: String,
        reader: Reader,
        length: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(
        columnIndex: Int,
        x: InputStream,
        length: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(
        columnIndex: Int,
        x: InputStream,
        length: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(
        columnIndex: Int,
        x: Reader,
        length: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(
        columnLabel: String,
        x: InputStream,
        length: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(
        columnLabel: String,
        x: InputStream,
        length: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(
        columnLabel: String,
        reader: Reader,
        length: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBlob(
        columnIndex: Int,
        inputStream: InputStream,
        length: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBlob(
        columnLabel: String,
        inputStream: InputStream,
        length: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateClob(
        columnIndex: Int,
        reader: Reader,
        length: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateClob(
        columnLabel: String,
        reader: Reader,
        length: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNClob(
        columnIndex: Int,
        reader: Reader,
        length: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNClob(
        columnLabel: String,
        reader: Reader,
        length: Long
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNCharacterStream(
        columnIndex: Int,
        x: Reader
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNCharacterStream(
        columnLabel: String,
        reader: Reader
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(
        columnIndex: Int,
        x: InputStream
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(
        columnIndex: Int,
        x: InputStream
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(
        columnIndex: Int,
        x: Reader
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateAsciiStream(
        columnLabel: String,
        x: InputStream
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBinaryStream(
        columnLabel: String,
        x: InputStream
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateCharacterStream(
        columnLabel: String,
        reader: Reader
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBlob(
        columnIndex: Int,
        inputStream: InputStream
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateBlob(
        columnLabel: String,
        inputStream: InputStream
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateClob(
        columnIndex: Int,
        reader: Reader
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateClob(
        columnLabel: String,
        reader: Reader
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNClob(
        columnIndex: Int,
        reader: Reader
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updateNClob(
        columnLabel: String,
        reader: Reader
    ) {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    private fun checkClosed() {
        if (isClosed) {
            throw SQLException("ResultSet was previously closed.")
        }
    }

    @Throws(SQLException::class)
    override fun <T> getObject(
        columnIndex: Int,
        type: Class<T>
    ): T {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun <T> getObject(
        columnLabel: String,
        type: Class<T>
    ): T {
        throw SQLFeatureNotSupportedException()
    }
}
