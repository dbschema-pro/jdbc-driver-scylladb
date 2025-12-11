package com.wisecoders.jdbc.scylladb

import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.Types

/**
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/scylladb-jdbc-driver).
 */
class ScyllaDBResultSetMetaData internal constructor(private val columnMetaData: List<ColumnMetaData>) :
    ResultSetMetaData {
    @Throws(SQLException::class)
    override fun <T> unwrap(iface: Class<T>): T {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun isWrapperFor(iface: Class<*>?): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    override fun getColumnCount(): Int {
        return columnMetaData.size
    }

    override fun isAutoIncrement(column: Int): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun isCaseSensitive(column: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun isSearchable(column: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun isCurrency(column: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    override fun isNullable(column: Int): Int {
        return ResultSetMetaData.columnNullable
    }

    @Throws(SQLException::class)
    override fun isSigned(column: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getColumnDisplaySize(column: Int): Int {
        throw SQLFeatureNotSupportedException()
    }

    override fun getColumnLabel(column: Int): String {
        return columnMetaData[column - 1].name
    }

    override fun getColumnName(column: Int): String {
        return columnMetaData[column - 1].name
    }

    override fun getSchemaName(column: Int): String {
        return getCatalogName(column)
    }

    override fun getPrecision(column: Int): Int {
        return 0 // todo
    }

    override fun getScale(column: Int): Int {
        return columnMetaData[column - 1].scale
    }

    override fun getTableName(column: Int): String {
        return columnMetaData[column - 1].tableName
    }

    override fun getCatalogName(column: Int): String {
        return columnMetaData[column - 1].keyspace
    }

    override fun getColumnType(column: Int): Int {
        return columnMetaData[column - 1].javaType
    }

    override fun getColumnTypeName(column: Int): String {
        return columnMetaData[column - 1].typeName
    }

    @Throws(SQLException::class)
    override fun isReadOnly(column: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun isWritable(column: Int): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun isDefinitelyWritable(column: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    override fun getColumnClassName(column: Int): String {
        return columnMetaData[column - 1].className!!
    }

    internal class ColumnMetaData(
        val name: String,
        val tableName: String,
        val keyspace: String,
        val typeName: String
    ) {
        val javaType: Int
            get() {
                val lower = toLowerCase(typeName)
                if (javaTypeMap.containsKey(
                        lower
                    )
                ) return javaTypeMap[lower]!!
                return Types.OTHER
            }

        val className: String?
            get() {
                val lower = toLowerCase(typeName)
                if (typeNameMap.containsKey(
                        lower
                    )
                ) return typeNameMap[lower]
                throw IllegalArgumentException("Type name is not known: $lower")
            }

        val scale: Int
            get() {
                val lower = toLowerCase(typeName)
                if (scaleMap.containsKey(
                        lower
                    )
                ) return scaleMap[lower]!!
                return 0
            }

        private fun toLowerCase(value: String): String {
            return value.lowercase()
        }

        companion object {
            private const val TYPE_MAP = 4999544
            private const val TYPE_LIST = 4999545

            private val javaTypeMap: MutableMap<String, Int> = HashMap()
            private val typeNameMap: MutableMap<String, String> = HashMap()
            private val scaleMap: MutableMap<String, Int> = HashMap()

            init {
                javaTypeMap["ascii"] =
                    Types.VARCHAR
                javaTypeMap["bigint"] =
                    Types.BIGINT
                javaTypeMap["blob"] =
                    Types.BLOB
                javaTypeMap["boolean"] =
                    Types.BOOLEAN
                javaTypeMap["counter"] =
                    Types.BIGINT
                javaTypeMap["date"] =
                    Types.DATE
                javaTypeMap["decimal"] =
                    Types.DECIMAL
                javaTypeMap["double"] =
                    Types.DOUBLE
                javaTypeMap["duration"] =
                    Types.JAVA_OBJECT
                javaTypeMap["float"] =
                    Types.FLOAT
                javaTypeMap["inet"] =
                    Types.JAVA_OBJECT
                javaTypeMap["int"] =
                    Types.INTEGER
                javaTypeMap["list"] =
                    TYPE_LIST
                javaTypeMap["map"] =
                    TYPE_MAP
                javaTypeMap["set"] =
                    Types.STRUCT
                javaTypeMap["smallint"] =
                    Types.SMALLINT
                javaTypeMap["text"] =
                    Types.VARCHAR
                javaTypeMap["timestamp"] =
                    Types.TIMESTAMP
                javaTypeMap["tuple"] =
                    Types.JAVA_OBJECT
                javaTypeMap["udt"] =
                    Types.JAVA_OBJECT
                javaTypeMap["uuid"] =
                    Types.JAVA_OBJECT
                javaTypeMap["time"] =
                    Types.TIME
                javaTypeMap["timeuuid"] =
                    Types.JAVA_OBJECT
                javaTypeMap["tinyint"] =
                    Types.TINYINT
                javaTypeMap["varchar"] =
                    Types.VARCHAR
                javaTypeMap["varint"] =
                    Types.INTEGER

                typeNameMap["ascii"] =
                    "java.lang.String"
                typeNameMap["bigint"] =
                    "java.lang.Long"
                typeNameMap["blob"] =
                    "java.lang.Byte[]"
                typeNameMap["boolean"] =
                    "java.lang.Boolean"
                typeNameMap["counter"] =
                    "java.lang.Long"
                typeNameMap["date"] =
                    "java.util.Date"
                typeNameMap["decimal"] =
                    "java.math.BigDecimal"
                typeNameMap["double"] =
                    "java.lang.Double"
                typeNameMap["duration"] =
                    "java.time.Duration"
                typeNameMap["float"] =
                    "java.lang.Float"
                typeNameMap["inet"] =
                    "java.net.InetAddress"
                typeNameMap["int"] =
                    "java.lang.Integer"
                typeNameMap["list"] =
                    "java.util.List"
                typeNameMap["map"] =
                    "java.util.Map"
                typeNameMap["set"] =
                    "java.lang.Set"
                typeNameMap["smallint"] =
                    "java.lang.Short"
                typeNameMap["text"] =
                    "java.lang.String"
                typeNameMap["timestamp"] =
                    "java.util.Date"
                typeNameMap["tuple"] =
                    "com.datastax.driver.core.TupleValue"
                typeNameMap["udt"] =
                    "com.datastax.driver.core.UDTValue"
                typeNameMap["uuid"] =
                    "java.util.UUID"
                typeNameMap["time"] =
                    "java.util.Time"
                typeNameMap["timeuuid"] =
                    "java.util.UUID"
                typeNameMap["tinyint"] =
                    "java.lang.Byte"
                typeNameMap["varchar"] =
                    "java.lang.String"
                typeNameMap["varint"] =
                    "java.math.BigInteger"

                scaleMap["timestamp"] =
                    3
                scaleMap["time"] = 9
                scaleMap["duration"] = 9
            }
        }
    }
}
