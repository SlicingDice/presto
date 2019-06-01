package io.prestosql.plugin.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ShannonDBPreparedStatement
        implements AutoCloseable
{
    private StringBuilder query;
    private HashMap<Integer, Object> params;
    private ShannonDBSocketClient shannonDBSocketClient;

    public ShannonDBPreparedStatement(ShannonDBSocketClient shannonDBSocketClient)
    {
        this.shannonDBSocketClient = shannonDBSocketClient;
    }

    public void addBatch()
    {
    }

    public void executeBatch()
    {
    }

    public void setObject(int index, Object value)
    {
        params.put(index, value);
    }

    private String prepareQuery()
    {
        Map<Integer, Object> reverseMap = new LinkedHashMap<>();

        params.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByKey()))
                .forEachOrdered(entry -> reverseMap.put(entry.getKey(), entry.getValue()));

        reverseMap.forEach((k, v) -> {
            int count = 0;
            for (int x = 0; x < query.length(); x++) {
                if (query.charAt(x) == '?') {
                    count++;

                    if (count == k) {
                        query.deleteCharAt(x);
                        query.insert(x, v);
                        continue;
                    }
                }
            }
        });

        return query.toString();
    }

    @Override
    public void close()
            throws Exception
    {
        params.clear();
        query = null;
    }

    public ShannonDBResultSet executeQuery()
    {
        return shannonDBSocketClient.execute(prepareQuery());
    }

    public static void setBoolean(ShannonDBPreparedStatement shannonDBPreparedStatement, int index, boolean value)
    {
        shannonDBPreparedStatement.getParams().put(index, value);
    }

    public void setByte(int index, byte value)
    {
        params.put(index, value);
    }

    public void setShort(int index, short value)
    {
        params.put(index, value);
    }

    public void setInt(int index, int value)
    {
        params.put(index, value);
    }

    public static void setLong(ShannonDBPreparedStatement shannonDBPreparedStatement, int index, long value)
    {
        shannonDBPreparedStatement.getParams().put(index, value);
    }

    public void setFloat(int index, float value)
    {
        params.put(index, value);
    }

    public static void setDouble(ShannonDBPreparedStatement shannonDBPreparedStatement, int index, double value)
    {
        shannonDBPreparedStatement.getParams().put(index, value);
    }

    public void setBigDecimal(int index, BigDecimal value)
    {
        params.put(index, value);
    }

    public void setString(int index, String value)
    {
        params.put(index, value);
    }

    public void setBytes(int index, byte[] value)
    {
        params.put(index, value);
    }

    public void setDate(int index, Date value)
    {
        params.put(index, value);
    }

    public void setTime(int index, Time value)
    {
        params.put(index, value);
    }

    public void setTimestamp(int index, Timestamp value)
    {
        params.put(index, value);
    }

    public void prepareQuery(String sql)
    {
        this.query = new StringBuilder(sql);
        this.params = new HashMap<>();
    }

    public HashMap<Integer, Object> getParams()
    {
        return params;
    }
}
