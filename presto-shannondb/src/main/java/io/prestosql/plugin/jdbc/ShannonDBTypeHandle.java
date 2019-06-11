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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ShannonDBTypeHandle
{
    private final String shannonDBType;
    private final Optional<String> shannonDBTypeName;
    private final int columnSize;
    private final int decimalDigits;
    private final Optional<Integer> arrayDimensions;

    @JsonCreator
    public ShannonDBTypeHandle(
            @JsonProperty("shannonDBType") String shannonDBType,
            @JsonProperty("shannonDBTypeName") Optional<String> shannonDBTypeName,
            @JsonProperty("columnSize") int columnSize,
            @JsonProperty("decimalDigits") int decimalDigits,
            @JsonProperty("arrayDimensions") Optional<Integer> arrayDimensions)
    {
        this.shannonDBType = shannonDBType;
        this.shannonDBTypeName = requireNonNull(shannonDBTypeName, "shannonDBTypeName is null");
        this.columnSize = columnSize;
        this.decimalDigits = decimalDigits;
        this.arrayDimensions = requireNonNull(arrayDimensions, "arrayDimensions is null");
    }

    @JsonProperty
    public String getShannonDBType()
    {
        return shannonDBType;
    }

    @JsonProperty
    public Optional<String> getShannonDBTypeName()
    {
        return shannonDBTypeName;
    }

    @JsonProperty
    public int getColumnSize()
    {
        return columnSize;
    }

    @JsonProperty
    public int getDecimalDigits()
    {
        return decimalDigits;
    }

    @JsonProperty
    public Optional<Integer> getArrayDimensions()
    {
        return arrayDimensions;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(shannonDBType, shannonDBTypeName, columnSize, decimalDigits, arrayDimensions);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShannonDBTypeHandle that = (ShannonDBTypeHandle) o;
        return shannonDBType == that.shannonDBType &&
                columnSize == that.columnSize &&
                decimalDigits == that.decimalDigits &&
                Objects.equals(shannonDBTypeName, that.shannonDBTypeName) &&
                Objects.equals(arrayDimensions, that.arrayDimensions);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("shannonDBType", shannonDBType)
                .add("shannonDBTypeName", shannonDBTypeName.orElse(null))
                .add("columnSize", columnSize)
                .add("decimalDigits", decimalDigits)
                .add("arrayDimensions", arrayDimensions.orElse(null))
                .toString();
    }
}
