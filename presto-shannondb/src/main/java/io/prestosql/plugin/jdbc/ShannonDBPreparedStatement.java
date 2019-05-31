package io.prestosql.plugin.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;

public class ShannonDBPreparedStatement implements AutoCloseable
{
    public void addBatch()
    {
    }

    public void executeBatch()
    {
    }

    public void setObject(int parameterIndex, Object o)
    {
    }

    @Override
    public void close()
            throws Exception
    {

    }

    public ResultSet executeQuery()
    {
    }

    public static void setBoolean(ShannonDBPreparedStatement shannonDBPreparedStatement, int i, boolean b)
    {
    }

    public void setByte(int index, byte b)
    {
    }

    public void setShort(int index, short i)
    {
    }

    public void setInt(int index, int i)
    {
    }

    public static void setLong(ShannonDBPreparedStatement shannonDBPreparedStatement, int i, long l)
    {
    }

    public void setFloat(int index, float v)
    {
    }

    public static void setDouble(ShannonDBPreparedStatement shannonDBPreparedStatement, int i, double v)
    {
    }

    public void setBigDecimal(int index, BigDecimal bigDecimal)
    {
    }

    public void setString(int index, String s)
    {
    }

    public void setBytes(int index, byte[] bytes)
    {
    }

    public void setDate(int index, Date date)
    {
    }

    public void setTime(int index, Time time)
    {
    }

    public void setTimestamp(int index, Timestamp timestamp)
    {
    }
}
