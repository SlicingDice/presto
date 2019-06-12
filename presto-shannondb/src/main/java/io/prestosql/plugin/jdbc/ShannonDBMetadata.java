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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.statistics.TableStatistics;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.Objects.requireNonNull;

public class ShannonDBMetadata
        implements ConnectorMetadata
{
    private final ShannonDBClient shannonDBClient;
    private final boolean allowDropTable;

    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    public ShannonDBMetadata(ShannonDBClient shannonDBClient, boolean allowDropTable)
    {
        this.shannonDBClient = requireNonNull(shannonDBClient, "client is null");
        this.allowDropTable = allowDropTable;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return shannonDBClient.schemaExists(ShannonDBIdentity.from(session), schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(shannonDBClient.getSchemaNames(ShannonDBIdentity.from(session)));
    }

    @Override
    public ShannonDBTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return shannonDBClient.getTableHandle(ShannonDBIdentity.from(session), tableName)
                .orElse(null);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        ShannonDBTableHandle handle = (ShannonDBTableHandle) table;

        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new ShannonDBTableHandle(
                handle.getSchemaTableName(),
                handle.getCatalogName(),
                handle.getSchemaName(),
                handle.getTableName(),
                newDomain,
                handle.getLimit(), handle.getColumnHandles());

        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary()));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        ShannonDBTableHandle handle = (ShannonDBTableHandle) table;

        if (!shannonDBClient.supportsLimit()) {
            return Optional.empty();
        }

        if (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        handle = new ShannonDBTableHandle(
                handle.getSchemaTableName(),
                handle.getCatalogName(),
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getConstraint(),
                OptionalLong.of(limit), handle.getColumnHandles());

        return Optional.of(new LimitApplicationResult<>(handle, shannonDBClient.isLimitGuaranteed()));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ShannonDBTableHandle handle = (ShannonDBTableHandle) table;

        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
        for (ShannonDBColumnHandle column : shannonDBClient.getColumns(session, handle)) {
            columnMetadata.add(column.getColumnMetadata());
        }
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata.build());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return shannonDBClient.getTableNames(ShannonDBIdentity.from(session), schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ShannonDBTableHandle shannonDBTableHandle = (ShannonDBTableHandle) tableHandle;

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (ShannonDBColumnHandle column : shannonDBClient.getColumns(session, shannonDBTableHandle)) {
            columnHandles.put(column.getColumnMetadata().getName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> tables = prefix.toOptionalSchemaTableName()
                .<List<SchemaTableName>>map(ImmutableList::of)
                .orElseGet(() -> listTables(session, prefix.getSchema()));
        for (SchemaTableName tableName : tables) {
            try {
                shannonDBClient.getTableHandle(ShannonDBIdentity.from(session), tableName)
                        .ifPresent(tableHandle -> columns.put(tableName, getTableMetadata(session, tableHandle).getColumns()));
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((ShannonDBColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (!allowDropTable) {
            throw new PrestoException(PERMISSION_DENIED, "DROP TABLE is disabled in this catalog");
        }
        ShannonDBTableHandle handle = (ShannonDBTableHandle) tableHandle;
        shannonDBClient.dropTable(ShannonDBIdentity.from(session), handle);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        ShannonDBOutputTableHandle handle = shannonDBClient.beginCreateTable(session, tableMetadata);
        setRollback(() -> shannonDBClient.rollbackCreateTable(ShannonDBIdentity.from(session), handle));
        return handle;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        shannonDBClient.createTable(session, tableMetadata);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        ShannonDBOutputTableHandle handle = (ShannonDBOutputTableHandle) tableHandle;
        shannonDBClient.commitCreateTable(ShannonDBIdentity.from(session), handle);
        clearRollback();
        return Optional.empty();
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "rollback action is already set");
    }

    private void clearRollback()
    {
        rollbackAction.set(null);
    }

    public void rollback()
    {
        Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ShannonDBOutputTableHandle handle = shannonDBClient.beginInsertTable(session, getTableMetadata(session, tableHandle));
        setRollback(() -> shannonDBClient.rollbackCreateTable(ShannonDBIdentity.from(session), handle));
        return handle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        ShannonDBOutputTableHandle jdbcInsertHandle = (ShannonDBOutputTableHandle) tableHandle;
        shannonDBClient.finishInsertTable(ShannonDBIdentity.from(session), jdbcInsertHandle);
        return Optional.empty();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle table, ColumnMetadata columnMetadata)
    {
        ShannonDBTableHandle tableHandle = (ShannonDBTableHandle) table;
        shannonDBClient.addColumn(session, tableHandle, columnMetadata);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column)
    {
        ShannonDBTableHandle tableHandle = (ShannonDBTableHandle) table;
        ShannonDBColumnHandle columnHandle = (ShannonDBColumnHandle) column;
        shannonDBClient.dropColumn(ShannonDBIdentity.from(session), tableHandle, columnHandle);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column, String target)
    {
        ShannonDBTableHandle tableHandle = (ShannonDBTableHandle) table;
        ShannonDBColumnHandle columnHandle = (ShannonDBColumnHandle) column;
        shannonDBClient.renameColumn(ShannonDBIdentity.from(session), tableHandle, columnHandle, target);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle table, SchemaTableName newTableName)
    {
        ShannonDBTableHandle tableHandle = (ShannonDBTableHandle) table;
        shannonDBClient.renameTable(ShannonDBIdentity.from(session), tableHandle, newTableName);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        ShannonDBTableHandle handle = (ShannonDBTableHandle) tableHandle;
        return shannonDBClient.getTableStatistics(session, handle, constraint.getSummary());
    }
}
