package io.prestosql.plugin.jdbc;

import java.net.Socket;
import java.util.Optional;
import java.util.Properties;

public class ShannonDBSocketClient implements AutoCloseable
{
    private Socket socket;

    public ShannonDBSocketClient connect(String connectionUrl, Properties connectionProperties)
    {
    }

    @Override
    public void close()
            throws Exception
    {
        
    }

    public ShannonDBResultSet getSchemas()
    {

    }

    public ShannonDBResultSet getColumns(ShannonDBTableHandle tableHandle)
    {
    }

    public boolean storesUpperCaseIdentifiers()
    {
    }

    public String getCatalog()
    {
    }

    public Optional<String> getSearchStringEscape()
    {
    }

    public ShannonDBResultSet getTables(String catalog, String schema, String table, String[] strings)
    {
    }

    public void execute(String query)
    {
    }

    public ShannonDBResultSet getColumns(String catalogName, String schema, String table, Object o)
    {
    }

    public ShannonDBPreparedStatement prepareStatement(String sql)
    {
    }

    public void commit()
    {
    }

    public void setAutoCommit(boolean b)
    {
    }

    public boolean isClosed()
    {
    }

    public void rollback()
    {
    }
}
