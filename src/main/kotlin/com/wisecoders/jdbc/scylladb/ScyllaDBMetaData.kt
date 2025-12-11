package com.wisecoders.jdbc.scylladb

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.wisecoders.common_jdbc.jvm.result_set.ArrayResultSet
import com.wisecoders.common_lib.common_slf4j.slf4jLogger
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.sql.RowIdLifetime
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException

/**
 * A Cassandra database is used as a catalogs by this driver. Schemas aren't used. A Cassandra collection is equivalent to a tables, in that each collection is a table.
 *
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/scylladb-jdbc-driver).
 */
class ScyllaDBMetaData internal constructor(
    private val connection: ScyllaDBConnection,
    private val driver: JdbcDriver,
) :
    DatabaseMetaData {
    override fun getSchemas(): ResultSet {
        return ArrayResultSet(listOf("TABLE_SCHEMA", "TABLE_CATALOG"))
    }

    /**
     * @see java.sql.DatabaseMetaData.getCatalogs
     */
    override fun getCatalogs(): ResultSet {
        val loadedKeyspaces: MutableList<String> = ArrayList()
        val retVal = ArrayResultSet()
        retVal.setColumnNames(listOf("TABLE_CAT"))
        for (identifier in connection.session.metadata.keyspaces.keys) {
            retVal.addRow(listOf(identifier.toString()))
            loadedKeyspaces.add(identifier.toString())
        }
        try {
            connection.prepareStatement("SELECT keyspace_name FROM system_schema.keyspaces").executeQuery().use { rs ->
                while (rs.next()) {
                    if (!loadedKeyspaces.contains(rs.getString(1))) {
                        retVal.addRow(listOf(rs.getString(1)))
                        loadedKeyspaces.add(rs.getString(1))
                    }
                }
            }
        } catch (ex: SQLException) {
            LOGGER.atWarn().setMessage("Error loading keyspaces using query 'SELECT keyspace_name FROM system_schema.keyspaces': $ex").log()
        }
        return retVal
    }

    override fun getTables(
        catalogName: String,
        schemaPattern: String?,
        tableNamePattern: String?,
        types: Array<String>?,
    ): ResultSet {
        val resultSet = ArrayResultSet()
        resultSet.setColumnNames(
            listOf(
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME",
                "REF_GENERATION"
            )
        )

        connection.session.metadata.getKeyspace(catalogName).ifPresent { metadata: KeyspaceMetadata ->
            for ((cqlIdentifier) in metadata.tables) {
                val data = listOf(
                    catalogName, // TABLE_CAT
                    "", // TABLE_SCHEM
                    cqlIdentifier.toString(), // TABLE_NAME
                    "TABLE", // TABLE_TYPE
                    null, // REMARKS
                    "", // TYPE_CAT
                    "", // TYPE_SCHEM
                    "", // TYPE_NAME
                    "", // SELF_REFERENCING_COL_NAME
                    "" // REF_GENERATION
                )

                resultSet.addRow(data)
            }
        }

        return resultSet
    }


    /**
     * @see java.sql.DatabaseMetaData.getColumns
     */
    override fun getColumns(
        catalogName: String,
        schemaName: String?,
        tableNamePattern: String?,
        columnNamePattern: String?,
    ): ResultSet {
        val result = ArrayResultSet()
        result.setColumnNames(
            listOf(
                "TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME",
                "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
                "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
                "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "OPTIONS"
            )
        )

        connection.session.metadata.getKeyspace(catalogName).ifPresent { metadata: KeyspaceMetadata ->
            if (tableNamePattern == null) {
                for (tableMetadata in metadata.tables.values) {
                    for (field in tableMetadata.columns.values) {
                        if (columnNamePattern == null || columnNamePattern == field.toString()) {
                            exportColumnsRecursive(tableMetadata, result, field)
                        }
                    }
                }
            } else {
                metadata.getTable(tableNamePattern)
                    .ifPresent { tableMetadata: TableMetadata ->
                        for (field in tableMetadata.columns.values) {
                            if (columnNamePattern == null || columnNamePattern == field.toString()) {
                                exportColumnsRecursive(tableMetadata, result, field)
                            }
                        }
                    }
            }
        }
        return result
    }

    private fun exportColumnsRecursive(
        tableMetadata: TableMetadata,
        result: ArrayResultSet,
        columnMetadata: ColumnMetadata,
    ) {
        val sb = StringBuilder()

        /*
        TableOptionsMetadata options = tableMetadata.getOptions();
        if( options != null ){
            sb.append("bloom_filter_fp_chance = " ).append(options.getBloomFilterFalsePositiveChance() ).
                    append("\n AND caching = '").append( options.getCaching()).append("'").
                    append("\n  AND comment = '").append( options.getComment()).append("'").
                    append("\n  AND compaction = ").append( options.getCompaction()).
                    append("\n  AND compression = ").append( options.getCompression()).
                    append("\n  AND dclocal_read_repair_chance = " ).append(options.getLocalReadRepairChance() ).
                    append("\n  AND default_time_to_live = " ).append(options.getDefaultTimeToLive() ).
                    append("\n  AND gc_grace_seconds = " ).append(options.getGcGraceInSeconds() ).
                    append("\n  AND max_index_interval = " ).append(options.getMaxIndexInterval() ).
                    append("\n  AND memtable_flush_period_in_ms = " ).append(options.getMemtableFlushPeriodInMs() ).
                    append("\n  AND min_index_interval = " ).append(options.getMinIndexInterval() ).
                    append("\n  AND read_repair_chance = " ).append(options.getReadRepairChance() ).
                    append("\n  AND speculative_retry = '").append(options.getSpeculativeRetry() ).append("'");
        }*/
        result.addRow(
            listOf(
                tableMetadata.keyspace.toString(),  // "TABLE_CAT",
                null,  // "TABLE_SCHEMA",
                tableMetadata.toString(),  // "TABLE_NAME", (i.e. Cassandra Collection Name)
                columnMetadata.toString(),  // "COLUMN_NAME",
                "" + columnMetadata.type,  // "DATA_TYPE",
                "" + columnMetadata.type,  // "TYPE_NAME", -- I LET THIS INTENTIONALLY TO USE .toString() BECAUSE OF USER DEFINED TYPES.
                "800",  // "COLUMN_SIZE",
                "0",  // "BUFFER_LENGTH", (not used)
                "0",  // "DECIMAL_DIGITS",
                "10",  // "NUM_PREC_RADIX",
                "0",  // "NULLABLE", // I RETREIVE HERE IF IS FROZEN ( MANDATORY ) OR NOT ( NULLABLE )
                "",  // "REMARKS",
                "",  // "COLUMN_DEF",
                "0",  // "SQL_DATA_TYPE", (not used)
                "0",  // "SQL_DATETIME_SUB", (not used)
                "800",  // "CHAR_OCTET_LENGTH",
                "1",  // "ORDINAL_POSITION",
                "NO",  // "IS_NULLABLE",
                null,  // "SCOPE_CATLOG", (not a REF type)
                null,  // "SCOPE_SCHEMA", (not a REF type)
                null,  // "SCOPE_TABLE", (not a REF type)
                null,  // "SOURCE_DATA_TYPE", (not a DISTINCT or REF type)
                "NO",  // "IS_AUTOINCREMENT" (can be auto-generated, but can also be specified)
                sb.toString() // TABLE_OPTIONS
            )
        )
    }


    /**
     * @see java.sql.DatabaseMetaData.getPrimaryKeys
     */
    override fun getPrimaryKeys(
        catalogName: String,
        schemaName: String,
        tableNamePattern: String,
    ): ResultSet {
        val result = ArrayResultSet()
        result.setColumnNames(listOf("TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"))

        connection.session.metadata.getKeyspace(catalogName).ifPresent { keyspaceMetadata: KeyspaceMetadata ->
            keyspaceMetadata.getTable(tableNamePattern)
                .ifPresent { tableMetadata: TableMetadata ->
                    for ((seq, columnMetadata) in tableMetadata.primaryKey.withIndex()) {
                        result.addRow(
                            listOf(
                                keyspaceMetadata.toString(),  // "TABLE_CAT",
                                null,  // "TABLE_SCHEMA",
                                tableMetadata.toString(),  // "TABLE_NAME", (i.e. Cassandra Collection Name)
                                columnMetadata.toString(),  // "COLUMN_NAME",
                                "" + seq,  // "ORDINAL_POSITION"
                                "PRIMARY KEY" // "PK_NAME"
                            )
                        )
                    }
                }
        }
        return result
    }

    /**
     * @see java.sql.DatabaseMetaData.getIndexInfo
     */
    override fun getIndexInfo(
        catalogName: String,
        schemaName: String,
        tableNamePattern: String,
        unique: Boolean,
        approximate: Boolean,
    ): ResultSet {
        val result = ArrayResultSet()
        result.setColumnNames(
            listOf(
                "TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE",
                "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION"
            )
        )

        connection.session.metadata.getKeyspace(catalogName).ifPresent { keyspaceMetadata: KeyspaceMetadata ->
            keyspaceMetadata.getTable(tableNamePattern)
                .ifPresent { tableMetadata: TableMetadata ->
                    var seq = 0
                    for ((key, value) in tableMetadata.clusteringColumns) {
                        result.addRow(
                            listOf(
                                keyspaceMetadata.toString(),  // "TABLE_CAT",
                                null,  // "TABLE_SCHEMA",
                                tableMetadata.name.toString(),  // "TABLE_NAME", (i.e. Cassandra Collection Name)
                                "FALSE",  // "NON-UNIQUE",
                                key.name.toString(),  // "INDEX QUALIFIER",
                                "CLUSTER KEY",  // "INDEX_NAME",
                                "0",  // "TYPE",
                                "" + seq++,  // "ORDINAL_POSITION"
                                key.name.toString(),  // "COLUMN_NAME",
                                if (value == ClusteringOrder.ASC) "A" else "D",  // "ASC_OR_DESC",
                                "0",  // "CARDINALITY",
                                "0",  // "PAGES",
                                "" // "FILTER_CONDITION",
                            )
                        )
                    }
                }
        }
        return result
    }

    @Throws(SQLException::class)
    override fun getTypeInfo(): ResultSet? {
        return null
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
    override fun allProceduresAreCallable(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun allTablesAreSelectable(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    override fun getURL(): String? {
        return null
    }

    override fun getUserName(): String? {
        return null
    }

    @Throws(SQLException::class)
    override fun isReadOnly(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun nullsAreSortedHigh(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun nullsAreSortedLow(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun nullsAreSortedAtStart(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun nullsAreSortedAtEnd(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    override fun getDatabaseProductName(): String {
        return "Cassandra"
    }

    override fun getDatabaseProductVersion(): String? {
        val result = connection.session.execute("select release_version from system.local")
        return result.one()!!.getString(0)
    }

    override fun getDriverName(): String {
        return "Cassandra JDBC Driver"
    }

    override fun getDriverVersion(): String {
        return driver.version
    }

    override fun getDriverMajorVersion(): Int {
        return driver.majorVersion
    }

    override fun getDriverMinorVersion(): Int {
        return driver.minorVersion
    }

    @Throws(SQLException::class)
    override fun usesLocalFiles(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun usesLocalFilePerTable(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    override fun supportsMixedCaseIdentifiers(): Boolean {
        return false
    }

    override fun storesUpperCaseIdentifiers(): Boolean {
        return false
    }

    override fun storesLowerCaseIdentifiers(): Boolean {
        return true
    }

    override fun storesMixedCaseIdentifiers(): Boolean {
        return false
    }

    override fun supportsMixedCaseQuotedIdentifiers(): Boolean {
        return true
    }

    override fun storesUpperCaseQuotedIdentifiers(): Boolean {
        return true
    }

    override fun storesLowerCaseQuotedIdentifiers(): Boolean {
        return true
    }

    override fun storesMixedCaseQuotedIdentifiers(): Boolean {
        return true
    }

    override fun getIdentifierQuoteString(): String {
        return "\""
    }

    override fun getSQLKeywords(): String? {
        return null
    }

    override fun getNumericFunctions(): String? {
        return null
    }

    override fun getStringFunctions(): String? {
        return null
    }

    override fun getSystemFunctions(): String? {
        return null
    }

    override fun getTimeDateFunctions(): String? {
        return null
    }

    override fun getSearchStringEscape(): String? {
        return null
    }

    override fun getExtraNameCharacters(): String {
        return ""
    }

    @Throws(SQLException::class)
    override fun supportsAlterTableWithAddColumn(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsAlterTableWithDropColumn(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsColumnAliasing(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun nullPlusNonNullIsNull(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsConvert(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsConvert(
        fromType: Int,
        toType: Int,
    ): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsTableCorrelationNames(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsDifferentTableCorrelationNames(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsExpressionsInOrderBy(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsOrderByUnrelated(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsGroupBy(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsGroupByUnrelated(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsGroupByBeyondSelect(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsLikeEscapeClause(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsMultipleResultSets(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsMultipleTransactions(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    override fun supportsNonNullableColumns(): Boolean {
        return true
    }

    @Throws(SQLException::class)
    override fun supportsMinimumSQLGrammar(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsCoreSQLGrammar(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsExtendedSQLGrammar(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsANSI92EntryLevelSQL(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsANSI92IntermediateSQL(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsANSI92FullSQL(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsIntegrityEnhancementFacility(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsOuterJoins(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsFullOuterJoins(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsLimitedOuterJoins(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getSchemaTerm(): String {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getProcedureTerm(): String {
        throw SQLFeatureNotSupportedException()
    }

    override fun getCatalogTerm(): String {
        return "keyspace"
    }

    @Throws(SQLException::class)
    override fun isCatalogAtStart(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    override fun getCatalogSeparator(): String {
        return "."
    }

    @Throws(SQLException::class)
    override fun supportsSchemasInDataManipulation(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsSchemasInProcedureCalls(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsSchemasInTableDefinitions(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsSchemasInIndexDefinitions(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsSchemasInPrivilegeDefinitions(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    override fun supportsCatalogsInDataManipulation(): Boolean {
        return true
    }

    @Throws(SQLException::class)
    override fun supportsCatalogsInProcedureCalls(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsCatalogsInTableDefinitions(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsCatalogsInIndexDefinitions(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsCatalogsInPrivilegeDefinitions(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsPositionedDelete(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsPositionedUpdate(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsSelectForUpdate(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsStoredProcedures(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsSubqueriesInComparisons(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsSubqueriesInExists(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsSubqueriesInIns(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsSubqueriesInQuantifieds(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsCorrelatedSubqueries(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsUnion(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsUnionAll(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsOpenCursorsAcrossCommit(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsOpenCursorsAcrossRollback(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsOpenStatementsAcrossCommit(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsOpenStatementsAcrossRollback(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxBinaryLiteralLength(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxCharLiteralLength(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxColumnNameLength(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxColumnsInGroupBy(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxColumnsInIndex(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxColumnsInOrderBy(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxColumnsInSelect(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxColumnsInTable(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxConnections(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxCursorNameLength(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxIndexLength(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxSchemaNameLength(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxProcedureNameLength(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxCatalogNameLength(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxRowSize(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun doesMaxRowSizeIncludeBlobs(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxStatementLength(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxStatements(): Int {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getMaxTableNameLength(): Int {
        throw SQLFeatureNotSupportedException()
    }

    override fun getMaxTablesInSelect(): Int {
        return 1
    }

    @Throws(SQLException::class)
    override fun getMaxUserNameLength(): Int {
        throw SQLFeatureNotSupportedException()
    }

    override fun getDefaultTransactionIsolation(): Int {
        return Connection.TRANSACTION_NONE
    }

    /**
     * Cassandra doesn't support transactions, but document updates are atomic.
     */
    override fun supportsTransactions(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun supportsTransactionIsolationLevel(level: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsDataDefinitionAndDataManipulationTransactions(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsDataManipulationTransactionsOnly(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun dataDefinitionCausesTransactionCommit(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun dataDefinitionIgnoredInTransactions(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    override fun getProcedures(
        catalogName: String,
        schemaPattern: String,
        procedureNamePattern: String,
    ): ResultSet? {
        return null
    }

    override fun getProcedureColumns(
        catalogName: String,
        schemaPattern: String,
        procedureNamePattern: String,
        columnNamePattern: String,
    ): ResultSet? {
        return null
    }

    override fun getTableTypes(): ResultSet? {
        return null
    }

    override fun getColumnPrivileges(
        catalogName: String,
        schemaName: String,
        table: String,
        columnNamePattern: String,
    ): ResultSet? {
        return null
    }

    override fun getTablePrivileges(
        catalogName: String,
        schemaPattern: String,
        tableNamePattern: String,
    ): ResultSet? {
        return null
    }

    override fun getBestRowIdentifier(
        catalogName: String,
        schemaName: String,
        table: String,
        scope: Int,
        nullable: Boolean,
    ): ResultSet? {
        return null
    }

    override fun getVersionColumns(
        catalogName: String,
        schemaName: String,
        table: String,
    ): ResultSet? {
        return null
    }

    override fun getExportedKeys(
        catalogName: String,
        schemaName: String?,
        tableNamePattern: String?,
    ): ResultSet? {
        return null
    }

    override fun getImportedKeys(
        catalogName: String,
        schemaName: String?,
        tableNamePattern: String?,
    ): ResultSet? {
        return null
    }


    override fun getCrossReference(
        parentCatalog: String,
        parentSchema: String?,
        parentTable: String?,
        foreignCatalog: String,
        foreignSchema: String?,
        foreignTable: String?,
    ): ResultSet? {
        return null
    }


    override fun supportsResultSetType(type: Int): Boolean {
        return type == ResultSet.TYPE_FORWARD_ONLY
    }

    @Throws(SQLException::class)
    override fun supportsResultSetConcurrency(
        type: Int,
        concurrency: Int,
    ): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun ownUpdatesAreVisible(type: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun ownDeletesAreVisible(type: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun ownInsertsAreVisible(type: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun othersUpdatesAreVisible(type: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun othersDeletesAreVisible(type: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun othersInsertsAreVisible(type: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun updatesAreDetected(type: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun deletesAreDetected(type: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun insertsAreDetected(type: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    override fun supportsBatchUpdates(): Boolean {
        return true
    }

    override fun getUDTs(
        catalogName: String,
        schemaPattern: String,
        typeNamePattern: String,
        types: IntArray,
    ): ResultSet? {
        return null
    }

    override fun getConnection(): Connection {
        return connection
    }

    override fun supportsSavepoints(): Boolean {
        return false
    }

    @Throws(SQLException::class)
    override fun supportsNamedParameters(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsMultipleOpenResults(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsGetGeneratedKeys(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    override fun getSuperTypes(
        catalogName: String,
        schemaPattern: String?,
        typeNamePattern: String?,
    ): ResultSet? {
        return null
    }

    override fun getSuperTables(
        catalogName: String,
        schemaPattern: String?,
        tableNamePattern: String?,
    ): ResultSet? {
        return null
    }

    override fun getAttributes(
        catalogName: String,
        schemaPattern: String?,
        typeNamePattern: String?,
        attributeNamePattern: String?,
    ): ResultSet? {
        return null
    }

    @Throws(SQLException::class)
    override fun supportsResultSetHoldability(holdability: Int): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getResultSetHoldability(): Int {
        throw SQLFeatureNotSupportedException()
    }

    override fun getDatabaseMajorVersion(): Int {
        return (databaseProductVersion!!.split("\\.".toRegex()).dropLastWhile { it.isEmpty() }
            .toTypedArray())[0].toInt()
    }

    override fun getDatabaseMinorVersion(): Int {
        return (databaseProductVersion!!.split("\\.".toRegex()).dropLastWhile { it.isEmpty() }
            .toTypedArray())[1].toInt()
    }

    override fun getJDBCMajorVersion(): Int {
        return 4
    }

    override fun getJDBCMinorVersion(): Int {
        return 2
    }

    override fun getSQLStateType(): Int {
        return DatabaseMetaData.sqlStateXOpen
    }

    @Throws(SQLException::class)
    override fun locatorsUpdateCopy(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun supportsStatementPooling(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getRowIdLifetime(): RowIdLifetime {
        throw SQLFeatureNotSupportedException()
    }

    override fun getSchemas(
        catalogName: String,
        schemaPattern: String,
    ): ResultSet? {
        return null
    }

    @Throws(SQLException::class)
    override fun supportsStoredFunctionsUsingCallSyntax(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun autoCommitFailureClosesAllResultSets(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    @Throws(SQLException::class)
    override fun getClientInfoProperties(): ResultSet {
        throw SQLFeatureNotSupportedException()
    }

    override fun getFunctions(
        catalogName: String,
        schemaPattern: String,
        functionNamePattern: String,
    ): ResultSet? {
        return null
    }

    override fun getFunctionColumns(
        catalogName: String,
        schemaPattern: String,
        functionNamePattern: String,
        columnNamePattern: String,
    ): ResultSet? {
        return null
    }

    override fun getPseudoColumns(
        catalogName: String,
        schemaPattern: String,
        tableNamePattern: String,
        columnNamePattern: String,
    ): ResultSet? {
        return null
    }

    @Throws(SQLException::class)
    override fun generatedKeyAlwaysReturned(): Boolean {
        throw SQLFeatureNotSupportedException()
    }

    companion object {
        val LOGGER = slf4jLogger()
    }
}
