package io.prestosql.plugin.jdbc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ShannonDBResultSet implements AutoCloseable
{
    private List<Map<String, Object>> list = new ArrayList<>();
    private int index = -1;

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
        Map<String, Object> map = list.get(index);
        Iterator<Object> iterator = map.values().iterator();
        Object lastValue = null;
        for (int x = 0; x <= field; x++){
           lastValue = iterator.next();
        }

        return lastValue == null;
    }

    public String getString(String key)
    {
        return (String) list.get(index).get(key);
    }

    public int getInt(String key)
    {
        return (int) list.get(index).get(key);
    }

}
