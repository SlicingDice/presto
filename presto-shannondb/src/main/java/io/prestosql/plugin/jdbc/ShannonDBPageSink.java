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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;

import java.sql.SQLNonTransientException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.jdbc.ShannonDBErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.ShannonDBErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class ShannonDBPageSink
        implements ConnectorPageSink
{
    private final ShannonDBSocketClient connection;
    private final ShannonDBPreparedStatement statement;

    private final List<Type> columnTypes;
    private final List<WriteFunction> columnWriters;
    private int batchSize;

    public ShannonDBPageSink(ConnectorSession session, ShannonDBOutputTableHandle handle, ShannonDBClient shannonDBClient)
    {
        try {
            connection = shannonDBClient.getShannonDBSocketClient(ShannonDBIdentity.from(session), handle);
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        try {
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(shannonDBClient.getShannonDBSocketClient(ShannonDBIdentity.from(session), handle), shannonDBClient.buildInsertSql(handle));
        }
        catch (Exception e) {
            closeWithSuppression(connection, e);
            throw new PrestoException(JDBC_ERROR, e);
        }

        columnTypes = handle.getColumnTypes();

        columnWriters = columnTypes.stream()
                .map(type -> {
                    WriteFunction writeFunction = shannonDBClient.toWriteMapping(session, type).getWriteFunction();
                    verify(
                            type.getJavaType() == writeFunction.getJavaType(),
                            "Presto type %s is not compatible with write function %s accepting %s",
                            type,
                            writeFunction,
                            writeFunction.getJavaType());
                    return writeFunction;
                })
                .collect(toImmutableList());
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try {
            for (int position = 0; position < page.getPositionCount(); position++) {
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    appendColumn(page, position, channel);
                }

                statement.addBatch();
                batchSize++;

                if (batchSize >= 1000) {
                    statement.executeBatch();
                    connection.commit();
                    connection.setAutoCommit(false);
                    batchSize = 0;
                }
            }
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        return NOT_BLOCKED;
    }

    private void appendColumn(Page page, int position, int channel)
            throws Exception
    {
        Block block = page.getBlock(channel);
        int parameterIndex = channel + 1;

        if (block.isNull(position)) {
            statement.setObject(parameterIndex, null);
            return;
        }

        Type type = columnTypes.get(channel);
        Class<?> javaType = type.getJavaType();
        WriteFunction writeFunction = columnWriters.get(channel);
        if (javaType == boolean.class) {
            ((BooleanWriteFunction) writeFunction).set(statement, parameterIndex, type.getBoolean(block, position));
        }
        else if (javaType == long.class) {
            ((LongWriteFunction) writeFunction).set(statement, parameterIndex, type.getLong(block, position));
        }
        else if (javaType == double.class) {
            ((DoubleWriteFunction) writeFunction).set(statement, parameterIndex, type.getDouble(block, position));
        }
        else if (javaType == Slice.class) {
            ((SliceWriteFunction) writeFunction).set(statement, parameterIndex, type.getSlice(block, position));
        }
        else if (javaType == Block.class) {
            ((BlockWriteFunction) writeFunction).set(statement, parameterIndex, (Block) type.getObject(block, position));
        }
        else {
            throw new VerifyException(format("Unexpected type %s with java type %s", type, javaType.getName()));
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // commit and close
        try (ShannonDBSocketClient connection = this.connection;
                ShannonDBPreparedStatement statement = this.statement) {
            if (batchSize > 0) {
                statement.executeBatch();
                connection.commit();
            }
        }
        catch (SQLNonTransientException e) {
            throw new PrestoException(JDBC_NON_TRANSIENT_ERROR, e);
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        // the committer does not need any additional info
        return completedFuture(ImmutableList.of());
    }

    @SuppressWarnings("unused")
    @Override
    public void abort()
    {
        // rollback and close
        try (ShannonDBSocketClient connection = this.connection;
                ShannonDBPreparedStatement statement = this.statement) {
            // skip rollback if implicitly closed due to an error
            if (!connection.isClosed()) {
                connection.rollback();
            }
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @SuppressWarnings("ObjectEquality")
    private static void closeWithSuppression(ShannonDBSocketClient connection, Throwable throwable)
    {
        try {
            connection.close();
        }
        catch (Throwable t) {
            // Self-suppression not permitted
            if (throwable != t) {
                throwable.addSuppressed(t);
            }
        }
    }
}
