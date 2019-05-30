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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.Type;

import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public final class ShannonDBColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final ShannonDBTypeHandle shannonDBTypeHandle;
    private final Type columnType;
    private final boolean nullable;

    @JsonCreator
    public ShannonDBColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("shannonDBTypeHandle") ShannonDBTypeHandle shannonDBTypeHandle,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("nullable") boolean nullable)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.shannonDBTypeHandle = requireNonNull(shannonDBTypeHandle, "shannonDBTypeHandle is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.nullable = nullable;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public ShannonDBTypeHandle getShannonDBTypeHandle()
    {
        return shannonDBTypeHandle;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public boolean isNullable()
    {
        return nullable;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType, nullable, null, null, false, emptyMap());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ShannonDBColumnHandle o = (ShannonDBColumnHandle) obj;
        return Objects.equals(this.columnName, o.columnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").join(
                columnName,
                columnType.getDisplayName(),
                shannonDBTypeHandle.getShannonDBTypeName());
    }
}
