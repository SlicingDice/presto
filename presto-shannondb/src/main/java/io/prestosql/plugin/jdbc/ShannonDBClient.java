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

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface ShannonDBClient
{
    default boolean schemaExists(ShannonDBIdentity identity, String schema)
    {
        return getSchemaNames(identity).contains(schema);
    }

    Set<String> getSchemaNames(ShannonDBIdentity identity);

    List<SchemaTableName> getTableNames(ShannonDBIdentity identity, Optional<String> schema);

    Optional<ShannonDBTableHandle> getTableHandle(ShannonDBIdentity identity, SchemaTableName schemaTableName);

    List<ShannonDBColumnHandle> getColumns(ConnectorSession session, ShannonDBTableHandle tableHandle);

    Optional<ColumnMapping> toPrestoType(ConnectorSession session, ShannonDBTypeHandle typeHandle);

    WriteMapping toWriteMapping(ConnectorSession session, Type type);

    ConnectorSplitSource getSplits(ShannonDBIdentity identity, ShannonDBTableHandle tableHandle);

    ShannonDBSocketClient getShannonDBSocketClient(ShannonDBIdentity identity, ShannonDBSplit split)
            throws Exception;

    default void abortReadSocket(ShannonDBSocketClient socket)
            throws Exception
    {
        // most drivers do not need this
    }

    ShannonDBPreparedStatement buildSql(ConnectorSession session, ShannonDBSocketClient socket, ShannonDBSplit split, ShannonDBTableHandle table, List<ShannonDBColumnHandle> columns)
            throws Exception;

    boolean supportsLimit();

    boolean isLimitGuaranteed();

    void addColumn(ConnectorSession session, ShannonDBTableHandle handle, ColumnMetadata column);

    void dropColumn(ShannonDBIdentity identity, ShannonDBTableHandle handle, ShannonDBColumnHandle column);

    void renameColumn(ShannonDBIdentity identity, ShannonDBTableHandle handle, ShannonDBColumnHandle jdbcColumn, String newColumnName);

    void renameTable(ShannonDBIdentity identity, ShannonDBTableHandle handle, SchemaTableName newTableName);

    void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    ShannonDBOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    void commitCreateTable(ShannonDBIdentity identity, ShannonDBOutputTableHandle handle);

    ShannonDBOutputTableHandle beginInsertTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    void finishInsertTable(ShannonDBIdentity identity, ShannonDBOutputTableHandle handle);

    void dropTable(ShannonDBIdentity identity, ShannonDBTableHandle shannonDBTableHandle);

    void rollbackCreateTable(ShannonDBIdentity identity, ShannonDBOutputTableHandle handle);

    String buildInsertSql(ShannonDBOutputTableHandle handle);

    ShannonDBSocketClient getShannonDBSocketClient(ShannonDBIdentity identity, ShannonDBOutputTableHandle handle)
            throws Exception;

    ShannonDBPreparedStatement getShannonDBPreparedStatement(ShannonDBSocketClient socket, String sql)
            throws Exception;

    TableStatistics getTableStatistics(ConnectorSession session, ShannonDBTableHandle handle, TupleDomain<ColumnHandle> tupleDomain);

    void abortReadConnection(ShannonDBSocketClient connection);
}
