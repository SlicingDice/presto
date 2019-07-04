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
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.predicate.TupleDomain.fromFixedValues;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ShannonDBSplitManager
        implements ConnectorSplitManager
{
    public static final String NODE_COLUMN_NAME = "node";

    private final ShannonDBClient shannonDBClient;
    private final NodeManager nodeManager;

    @Inject
    public ShannonDBSplitManager(ShannonDBClient shannonDBClient, NodeManager nodeManager)
    {

        this.shannonDBClient = requireNonNull(shannonDBClient, "client is null");
        this.nodeManager = requireNonNull(nodeManager, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        List<ConnectorSplit> splits = nodeManager.getAllNodes().stream()
                .map(node -> new ShannonDBSplit(ImmutableList.of(node.getHostAndPort()), Optional.empty()))
                .collect(toList());

        return new FixedSplitSource(splits);
    }
}
