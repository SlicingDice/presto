/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.jdbc;

import com.google.common.base.CharMatcher;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.jdbc.ShannonDBErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.dateWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.jdbcTypeToPrestoType;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BaseShannonDBClient
        implements ShannonDBClient
{
    private static final Logger log = Logger.get(BaseShannonDBClient.class);

    private static final Map<Type, WriteMapping> WRITE_MAPPINGS = ImmutableMap.<Type, WriteMapping>builder()
            .put(BOOLEAN, WriteMapping.booleanMapping("boolean", booleanWriteFunction()))
            .put(BIGINT, WriteMapping.longMapping("bigint", bigintWriteFunction()))
            .put(INTEGER, WriteMapping.longMapping("integer", integerWriteFunction()))
            .put(SMALLINT, WriteMapping.longMapping("smallint", smallintWriteFunction()))
            .put(TINYINT, WriteMapping.longMapping("tinyint", tinyintWriteFunction()))
            .put(DOUBLE, WriteMapping.doubleMapping("double precision", doubleWriteFunction()))
            .put(REAL, WriteMapping.longMapping("real", realWriteFunction()))
            .put(VARBINARY, WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction()))
            .put(DATE, WriteMapping.longMapping("date", dateWriteFunction()))
            .build();

    protected final SocketFactory shannonDBSocketClientFactory;
    protected final String identifierQuote;
    protected final boolean caseInsensitiveNameMatching;
    protected final Cache<ShannonDBIdentity, Map<String, String>> remoteSchemaNames;
    protected final Cache<RemoteTableNameCacheKey, Map<String, String>> remoteTableNames;

    public BaseShannonDBClient(BaseShannonDBConfig config, String identifierQuote, SocketFactory socketFactory)
    {
        requireNonNull(config, "config is null"); // currently unused, retained as parameter for future extensions
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
        this.shannonDBSocketClientFactory = requireNonNull(socketFactory, "socketFactory is null");

        this.caseInsensitiveNameMatching = config.isCaseInsensitiveNameMatching();
        CacheBuilder<Object, Object> remoteNamesCacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getCaseInsensitiveNameMatchingCacheTtl().toMillis(), MILLISECONDS);
        this.remoteSchemaNames = remoteNamesCacheBuilder.build();
        this.remoteTableNames = remoteNamesCacheBuilder.build();
    }

    @PreDestroy
    public void destroy()
            throws Exception
    {
        shannonDBSocketClientFactory.close();
    }

    @Override
    public final Set<String> getSchemaNames(ShannonDBIdentity identity)
    {
        try (ShannonDBSocketClient shannonDBSocketClient = shannonDBSocketClientFactory.openSocket(identity)) {
            return listSchemas(shannonDBSocketClient).stream()
                    .map(schemaName -> schemaName.toLowerCase(ENGLISH))
                    .collect(toImmutableSet());
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected Collection<String> listSchemas(ShannonDBSocketClient shannonDBSocketClient)
    {
        try (ShannonDBResultSet resultSet = shannonDBSocketClient.getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (!schemaName.equalsIgnoreCase("information_schema")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public List<SchemaTableName> getTableNames(ShannonDBIdentity identity, Optional<String> schema)
    {
        try (ShannonDBSocketClient shannonDBSocketClient = shannonDBSocketClientFactory.openSocket(identity)) {
            Optional<String> remoteSchema = schema.map(schemaName -> toRemoteSchemaName(identity, shannonDBSocketClient, schemaName));
            try (ShannonDBResultSet resultSet = getTables(shannonDBSocketClient, remoteSchema, Optional.empty())) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    String tableSchema = getTableSchemaName(resultSet);
                    String tableName = resultSet.getString("TABLE_NAME");
                    list.add(new SchemaTableName(tableSchema.toLowerCase(ENGLISH), tableName.toLowerCase(ENGLISH)));
                }
                return list.build();
            }
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<ShannonDBTableHandle> getTableHandle(ShannonDBIdentity identity, SchemaTableName schemaTableName)
    {
        try (ShannonDBSocketClient shannonDBSocketClient = shannonDBSocketClientFactory.openSocket(identity)) {
            String remoteSchema = toRemoteSchemaName(identity, shannonDBSocketClient, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(identity, shannonDBSocketClient, remoteSchema, schemaTableName.getTableName());
            try (ShannonDBResultSet resultSet = getTables(shannonDBSocketClient, Optional.of(remoteSchema), Optional.of(remoteTable))) {
                List<ShannonDBTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new ShannonDBTableHandle(
                            schemaTableName,
                            resultSet.getString("TABLE_CAT"),
                            resultSet.getString("TABLE_SCHEM"),
                            resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return Optional.empty();
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return Optional.of(getOnlyElement(tableHandles));
            }
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public List<ShannonDBColumnHandle> getColumns(ConnectorSession session, ShannonDBTableHandle tableHandle)
    {
        try (ShannonDBSocketClient shannonDBSocketClient = shannonDBSocketClientFactory.openSocket(ShannonDBIdentity.from(session))) {
            try (ShannonDBResultSet resultSet = shannonDBSocketClient.getColumns(tableHandle)) {
                List<ShannonDBColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    ShannonDBTypeHandle typeHandle = new ShannonDBTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            Optional.ofNullable(resultSet.getString("TYPE_NAME")),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"),
                            Optional.empty());
                    Optional<ColumnMapping> columnMapping = toPrestoType(session, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                        columns.add(new ShannonDBColumnHandle(columnName, typeHandle, columnMapping.get().getType(), nullable));
                    }
                }
                if (columns.isEmpty()) {
                    // In rare cases (e.g. PostgreSQL) a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, ShannonDBTypeHandle typeHandle)
    {
        return jdbcTypeToPrestoType(session, typeHandle);
    }

    @Override
    public ConnectorSplitSource getSplits(ShannonDBIdentity identity, ShannonDBTableHandle tableHandle)
    {
        return new FixedSplitSource(ImmutableList.of(new ShannonDBSplit(Optional.empty())));
    }

    @Override
    public ShannonDBSocketClient getShannonDBSocketClient(ShannonDBIdentity identity, ShannonDBSplit split)
            throws Exception
    {
        ShannonDBSocketClient shannonDBSocketClient = shannonDBSocketClientFactory.openSocket(identity);
        return shannonDBSocketClient;
    }

    @Override
    public ShannonDBPreparedStatement buildSql(ConnectorSession session, ShannonDBSocketClient shannonDBSocketClient, ShannonDBSplit split, ShannonDBTableHandle table, List<ShannonDBColumnHandle> columns)
            throws Exception
    {
        return new QueryBuilder(identifierQuote).buildSql(
                this,
                session,
                shannonDBSocketClient,
                table.getCatalogName(),
                table.getSchemaName(),
                table.getTableName(),
                columns,
                table.getConstraint(),
                split.getAdditionalPredicate(),
                tryApplyLimit(table.getLimit()));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public ShannonDBOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return beginWriteTable(session, tableMetadata);
    }

    @Override
    public ShannonDBOutputTableHandle beginInsertTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return beginWriteTable(session, tableMetadata);
    }

    private ShannonDBOutputTableHandle beginWriteTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            return createTable(session, tableMetadata, generateTemporaryTableName());
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected ShannonDBOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String tableName)
            throws Exception
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();

        ShannonDBIdentity identity = ShannonDBIdentity.from(session);
        if (!getSchemaNames(identity).contains(schemaTableName.getSchemaName())) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schemaTableName.getSchemaName());
        }

        try (ShannonDBSocketClient shannonDBSocketClient = shannonDBSocketClientFactory.openSocket(identity)) {
            boolean uppercase = shannonDBSocketClient.storesUpperCaseIdentifiers();
            String remoteSchema = toRemoteSchemaName(identity, shannonDBSocketClient, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(identity, shannonDBSocketClient, remoteSchema, schemaTableName.getTableName());
            if (uppercase) {
                tableName = tableName.toUpperCase(ENGLISH);
            }
            String catalog = shannonDBSocketClient.getCatalog();

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                // TODO in INSERT case, we should reuse original column type and, ideally, constraints (then ShannonDBPageSink must get writer from toPrestoType())
                columnList.add(getColumnSql(session, column, columnName));
            }

            String sql = format(
                    "CREATE TABLE %s (%s)",
                    quoted(catalog, remoteSchema, tableName),
                    join(", ", columnList.build()));
            execute(shannonDBSocketClient, sql);

            return new ShannonDBOutputTableHandle(
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    tableName);
        }
    }

    private String getColumnSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        StringBuilder sb = new StringBuilder()
                .append(quoted(columnName))
                .append(" ")
                .append(toWriteMapping(session, column.getType()).getDataType());
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    protected String generateTemporaryTableName()
    {
        return "tmp_presto_" + UUID.randomUUID().toString().replace("-", "");
    }

    @Override
    public void commitCreateTable(ShannonDBIdentity identity, ShannonDBOutputTableHandle handle)
    {
        renameTable(
                identity,
                handle.getCatalogName(),
                handle.getSchemaName(),
                handle.getTemporaryTableName(),
                new SchemaTableName(handle.getSchemaName(), handle.getTableName()));
    }

    @Override
    public void renameTable(ShannonDBIdentity identity, ShannonDBTableHandle handle, SchemaTableName newTableName)
    {
        renameTable(identity, handle.getCatalogName(), handle.getSchemaName(), handle.getTableName(), newTableName);
    }

    protected void renameTable(ShannonDBIdentity identity, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        try (ShannonDBSocketClient shannonDBSocketClient = shannonDBSocketClientFactory.openSocket(identity)) {
            String newSchemaName = newTable.getSchemaName();
            String newTableName = newTable.getTableName();
            if (shannonDBSocketClient.storesUpperCaseIdentifiers()) {
                newSchemaName = newSchemaName.toUpperCase(ENGLISH);
                newTableName = newTableName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s RENAME TO %s",
                    quoted(catalogName, schemaName, tableName),
                    quoted(catalogName, newSchemaName, newTableName));
            execute(shannonDBSocketClient, sql);
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void finishInsertTable(ShannonDBIdentity identity, ShannonDBOutputTableHandle handle)
    {
        String temporaryTable = quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName());
        String targetTable = quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName());
        String insertSql = format("INSERT INTO %s SELECT * FROM %s", targetTable, temporaryTable);
        String cleanupSql = "DROP TABLE " + temporaryTable;

        try (ShannonDBSocketClient shannonDBSocketClient = getShannonDBSocketClient(identity, handle)) {
            execute(shannonDBSocketClient, insertSql);
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        try (ShannonDBSocketClient shannonDBSocketClient = getShannonDBSocketClient(identity, handle)) {
            execute(shannonDBSocketClient, cleanupSql);
        }
        catch (Exception e) {
            log.warn(e, "Failed to cleanup temporary table: %s", temporaryTable);
        }
    }

    @Override
    public void addColumn(ConnectorSession session, ShannonDBTableHandle handle, ColumnMetadata column)
    {
        try (ShannonDBSocketClient shannonDBSocketClient = shannonDBSocketClientFactory.openSocket(ShannonDBIdentity.from(session))) {
            String columnName = column.getName();
            if (shannonDBSocketClient.storesUpperCaseIdentifiers()) {
                columnName = columnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s ADD %s",
                    quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
                    getColumnSql(session, column, columnName));
            execute(shannonDBSocketClient, sql);
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameColumn(ShannonDBIdentity identity, ShannonDBTableHandle handle, ShannonDBColumnHandle jdbcColumn, String newColumnName)
    {
        try (ShannonDBSocketClient shannonDBSocketClient = shannonDBSocketClientFactory.openSocket(identity)) {
            if (shannonDBSocketClient.storesUpperCaseIdentifiers()) {
                newColumnName = newColumnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s RENAME COLUMN %s TO %s",
                    quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
                    jdbcColumn.getColumnName(),
                    newColumnName);
            execute(shannonDBSocketClient, sql);
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void dropColumn(ShannonDBIdentity identity, ShannonDBTableHandle handle, ShannonDBColumnHandle column)
    {
        try (ShannonDBSocketClient shannonDBSocketClient = shannonDBSocketClientFactory.openSocket(identity)) {
            String sql = format(
                    "ALTER TABLE %s DROP COLUMN %s",
                    quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
                    column.getColumnName());
            execute(shannonDBSocketClient, sql);
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void dropTable(ShannonDBIdentity identity, ShannonDBTableHandle handle)
    {
        String sql = "DROP TABLE " + quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName());

        try (ShannonDBSocketClient shannonDBSocketClient = shannonDBSocketClientFactory.openSocket(identity)) {
            execute(shannonDBSocketClient, sql);
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void rollbackCreateTable(ShannonDBIdentity identity, ShannonDBOutputTableHandle handle)
    {
        dropTable(identity, new ShannonDBTableHandle(
                new SchemaTableName(handle.getSchemaName(), handle.getTemporaryTableName()),
                handle.getCatalogName(),
                handle.getSchemaName(),
                handle.getTemporaryTableName()));
    }

    @Override
    public String buildInsertSql(ShannonDBOutputTableHandle handle)
    {
        return format(
                "INSERT INTO %s VALUES (%s)",
                quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()),
                join(",", nCopies(handle.getColumnNames().size(), "?")));
    }

    @Override
    public ShannonDBSocketClient getShannonDBSocketClient(ShannonDBIdentity identity, ShannonDBOutputTableHandle handle)
            throws Exception
    {
        return shannonDBSocketClientFactory.openSocket(identity);
    }

    @Override
    public ShannonDBPreparedStatement getShannonDBPreparedStatement(ShannonDBSocketClient shannonDBSocketClient, String sql)
            throws Exception
    {
        return shannonDBSocketClient.prepareStatement(sql);
    }

    protected ShannonDBResultSet getTables(ShannonDBSocketClient shannonDBSocketClient, Optional<String> schemaName, Optional<String> tableName)
            throws Exception
    {
        Optional<String> escape = shannonDBSocketClient.getSearchStringEscape();
        return shannonDBSocketClient.getTables(
                shannonDBSocketClient.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"TABLE", "VIEW"});
    }

    protected String getTableSchemaName(ShannonDBResultSet resultSet)
            throws Exception
    {
        return resultSet.getString("TABLE_SCHEM");
    }

    protected String toRemoteSchemaName(ShannonDBIdentity identity, ShannonDBSocketClient shannonDBSocketClient, String schemaName)
    {
        requireNonNull(schemaName, "schemaName is null");
        verify(CharMatcher.forPredicate(Character::isUpperCase).matchesNoneOf(schemaName), "Expected schema name from internal metadata to be lowercase: %s", schemaName);

        if (caseInsensitiveNameMatching) {
            try {
                Map<String, String> mapping = remoteSchemaNames.getIfPresent(identity);
                if (mapping != null && !mapping.containsKey(schemaName)) {
                    // This might be a schema that has just been created. Force reload.
                    mapping = null;
                }
                if (mapping == null) {
                    mapping = listSchemasByLowerCase(shannonDBSocketClient);
                    remoteSchemaNames.put(identity, mapping);
                }
                String remoteSchema = mapping.get(schemaName);
                if (remoteSchema != null) {
                    return remoteSchema;
                }
            }
            catch (RuntimeException e) {
                throw new PrestoException(JDBC_ERROR, "Failed to find remote schema name: " + firstNonNull(e.getMessage(), e), e);
            }
        }

        try {
            if (shannonDBSocketClient.storesUpperCaseIdentifiers()) {
                return schemaName.toUpperCase(ENGLISH);
            }
            return schemaName;
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected Map<String, String> listSchemasByLowerCase(ShannonDBSocketClient shannonDBSocketClient)
    {
        return listSchemas(shannonDBSocketClient).stream()
                .collect(toImmutableMap(schemaName -> schemaName.toLowerCase(ENGLISH), schemaName -> schemaName));
    }

    protected String toRemoteTableName(ShannonDBIdentity identity, ShannonDBSocketClient shannonDBSocketClient, String remoteSchema, String tableName)
    {
        requireNonNull(remoteSchema, "remoteSchema is null");
        requireNonNull(tableName, "tableName is null");
        verify(CharMatcher.forPredicate(Character::isUpperCase).matchesNoneOf(tableName), "Expected table name from internal metadata to be lowercase: %s", tableName);

        if (caseInsensitiveNameMatching) {
            try {
                RemoteTableNameCacheKey cacheKey = new RemoteTableNameCacheKey(identity, remoteSchema);
                Map<String, String> mapping = remoteTableNames.getIfPresent(cacheKey);
                if (mapping != null && !mapping.containsKey(tableName)) {
                    // This might be a table that has just been created. Force reload.
                    mapping = null;
                }
                if (mapping == null) {
                    mapping = listTablesByLowerCase(shannonDBSocketClient, remoteSchema);
                    remoteTableNames.put(cacheKey, mapping);
                }
                String remoteTable = mapping.get(tableName);
                if (remoteTable != null) {
                    return remoteTable;
                }
            }
            catch (RuntimeException e) {
                throw new PrestoException(JDBC_ERROR, "Failed to find remote table name: " + firstNonNull(e.getMessage(), e), e);
            }
        }

        try {
            if (shannonDBSocketClient.storesUpperCaseIdentifiers()) {
                return tableName.toUpperCase(ENGLISH);
            }
            return tableName;
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected Map<String, String> listTablesByLowerCase(ShannonDBSocketClient shannonDBSocketClient, String remoteSchema)
    {
        try (ShannonDBResultSet resultSet = getTables(shannonDBSocketClient, Optional.of(remoteSchema), Optional.empty())) {
            ImmutableMap.Builder<String, String> map = ImmutableMap.builder();
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                map.put(tableName.toLowerCase(ENGLISH), tableName);
            }
            return map.build();
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ShannonDBTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        return TableStatistics.empty();
    }

    @Override
    public void abortReadConnection(ShannonDBSocketClient connection)
    {

    }

    protected void execute(ShannonDBSocketClient shannonDBSocketClient, String query)
            throws Exception
    {
        log.debug("Execute: %s", query);
        shannonDBSocketClient.execute(query);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction());
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.sliceMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        WriteMapping writeMapping = WRITE_MAPPINGS.get(type);
        if (writeMapping != null) {
            return writeMapping;
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    private Function<String, String> tryApplyLimit(OptionalLong limit)
    {
        if (!limit.isPresent()) {
            return Function.identity();
        }
        return limitFunction()
                .map(limitFunction -> (Function<String, String>) sql -> limitFunction.apply(sql, limit.getAsLong()))
                .orElseGet(Function::identity);
    }

    @Override
    public boolean supportsLimit()
    {
        return limitFunction().isPresent();
    }

    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.empty();
    }

    @Override
    public boolean isLimitGuaranteed()
    {
        throw new PrestoException(JDBC_ERROR, "limitFunction() is implemented without isLimitGuaranteed()");
    }

    protected String quoted(String name)
    {
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    protected String quoted(String catalog, String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            sb.append(quoted(catalog)).append(".");
        }
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
    }

    protected static Optional<String> escapeNamePattern(Optional<String> name, Optional<String> escape)
    {
        if (!name.isPresent() || !escape.isPresent()) {
            return name;
        }
        return Optional.of(escapeNamePattern(name.get(), escape.get()));
    }

    private static String escapeNamePattern(String name, String escape)
    {
        requireNonNull(name, "name is null");
        requireNonNull(escape, "escape is null");
        checkArgument(!escape.equals("_"), "Escape string must not be '_'");
        checkArgument(!escape.equals("%"), "Escape string must not be '%'");
        name = name.replace(escape, escape + escape);
        name = name.replace("_", escape + "_");
        name = name.replace("%", escape + "%");
        return name;
    }

    protected static ShannonDBResultSet getColumns(ShannonDBTableHandle tableHandle, ShannonDBSocketClient shannonDBSocketClient)
            throws Exception
    {
        Optional<String> escape = shannonDBSocketClient.getSearchStringEscape();
        return shannonDBSocketClient.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(Optional.ofNullable(tableHandle.getSchemaName()), escape).orElse(null),
                escapeNamePattern(Optional.ofNullable(tableHandle.getTableName()), escape).orElse(null),
                null);
    }
}
