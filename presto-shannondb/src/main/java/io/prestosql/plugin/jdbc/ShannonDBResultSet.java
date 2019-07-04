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

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ShannonDBResultSet implements AutoCloseable
{
    private final List<Map<Object, Object>> list;
    private final List<ShannonDBColumnHandle> columns;
    private int index = -1;

    public ShannonDBResultSet(final List<Map<Object, Object>> list){
        this.list = list;
        this.columns = null;
    }

    public ShannonDBResultSet(final List<Map<Object, Object>> list, final List<ShannonDBColumnHandle> columns)
    {
        this.list = list;
        this.columns = columns;
    }

    @Override
    public void close()
            throws Exception
    {
        list.clear();
    }

    public boolean next()
    {
        if (index + 1 < list.size()){
            index++;
            return true;
        }
        return false;
    }

    public boolean wasNull(int field)
    {
        final Map<Object, Object> map = list.get(index);
        Iterator<Object> iterator = map.values().iterator();
        Object lastValue = null;
        for (int x = 0; x <= field; x++){
           lastValue = iterator.next();
        }

        return lastValue == null;
    }

    public Object getColumnName(int columnIndex){
        if (columns != null && !columns.isEmpty()){
            return columns.get(columnIndex -1).getColumnName();
        }

        return null;
    }

    public String getString(String key)
    {
        return (String) list.get(index).get(key);
    }

    public int getInt(String key)
    {
        return (int) list.get(index).get(key);
    }

    public String getString(int columnIndex)
    {
        return (String) list.get(index).get(getColumnName(columnIndex));
    }

    public boolean getBoolean(int columnIndex)
    {
        return (boolean) list.get(index).get(getColumnName(columnIndex));
    }

    public float getFloat(int columnIndex)
    {
        return (float) list.get(index).get(getColumnName(columnIndex));
    }

    public BigDecimal getBigDecimal(int columnIndex)
    {
        return (BigDecimal) list.get(index).get(getColumnName(columnIndex));
    }

    public byte getBytes(int columnIndex)
    {
        return (byte) list.get(index).get(getColumnName(columnIndex));
    }

    public Date getDate(int columnIndex)
    {
        return (Date) list.get(index).get(getColumnName(columnIndex));
    }

    public Time getTime(int columnIndex)
    {
        return (Time) list.get(index).get(getColumnName(columnIndex));
    }

    public Timestamp getTimestamp(int columnIndex)
    {
        return (Timestamp) list.get(index).get(getColumnName(columnIndex));
    }

    public LocalDateTime getObject(int columnIndex, Class<LocalDateTime> localDateTimeClass)
    {
        return (LocalDateTime) list.get(index).get(getColumnName(columnIndex));
    }

    public long getByte(int columnIndex)
    {
        return (long) list.get(index).get(getColumnName(columnIndex));
    }

    public long getShort(int columnIndex)
    {
        return (long) list.get(index).get(getColumnName(columnIndex));
    }

    public long getInt(int columnIndex)
    {
        return Long.parseLong(list.get(index).get(getColumnName(columnIndex)).toString());
    }

    public long getLong(int columnIndex)
    {
        return (long) list.get(index).get(getColumnName(columnIndex));
    }

    public double getDouble(int columnIndex)
    {
        return (float) list.get(index).get(getColumnName(columnIndex));
    }

    public BigDecimal getBigDecimal(String columnName)
    {
        return (BigDecimal) list.get(index).get(columnName);
    }

    public boolean getBoolean(String columnName)
    {
        return (boolean) list.get(index).get(columnName);
    }

    public Date getDate(String columnName)
    {
        return (Date) list.get(index).get(columnName);
    }

    public Timestamp getTimestamp(String columnName)
    {
        return (Timestamp) list.get(index).get(columnName);
    }
}
