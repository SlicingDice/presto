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

import com.facebook.presto.jdbc.internal.jackson.databind.ObjectMapper;
import org.xerial.snappy.Snappy;

import javax.inject.Inject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class ShannonDBSocketClient
        implements AutoCloseable
{
    private Socket socket;
    private Properties connectionProperties;
    private ShannonDBMysqlReader mysqlReader;

    @Inject
    public ShannonDBSocketClient(){
    }

    public ShannonDBSocketClient connect(Properties connectionProperties)
    {
        this.connectionProperties = connectionProperties;
        this.mysqlReader = new ShannonDBMysqlReader();
        if (socket == null || socket.isClosed()) {
            try {
                socket = new Socket("179.95.190.123", 1234);
            }
            catch (java.io.IOException e) {
                e.printStackTrace();
            }
        }
        return this;
    }

    @Override
    public void close()
            throws Exception
    {
        socket.close();
    }

    public ShannonDBResultSet send(SocketRequest request)
    {
        try {

            DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
            DataInputStream inputStream = new DataInputStream(socket.getInputStream());

            final long requestId = 1;

            ObjectMapper objectMapper = new ObjectMapper();
            byte[] compressedJson = Snappy.compress("sql " + objectMapper.writeValueAsString(request));
            outputStream.writeLong(requestId);
            outputStream.writeInt(compressedJson.length);
            outputStream.write(compressedJson);
            outputStream.flush();

            final long responseId = inputStream.readLong();

            if (responseId != requestId) {
                return null;
            }

            final int length = inputStream.readInt();

            final byte[] compressedBytes = new byte[length];
            inputStream.read(compressedBytes);

            final byte[] messageBytes = Snappy.uncompress(compressedBytes);

            String data = new String(messageBytes, Charset.forName("UTF-8"));

            List<Map<String, Object>> list = objectMapper.readValue(data, List.class);

            return new ShannonDBResultSet(list);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ShannonDBResultSet getSchemas()
    {
        return mysqlReader.getSchemas(connectionProperties);
    }

    public boolean storesUpperCaseIdentifiers()
    {
        return true;
    }

    public String getCatalog()
    {
        return connectionProperties.getProperty("catalog");
    }

    public Optional<String> getSearchStringEscape()
    {
        return Optional.ofNullable(null);
    }

    public ShannonDBResultSet getTables(String catalog, String schema, String table, String[] strings)
    {
        return mysqlReader.getTables(catalog, schema, table, connectionProperties);
    }

    public ShannonDBResultSet execute(String query)
    {
        SocketRequest request = new SocketRequest();
        request.setProject_id("" + connectionProperties.getProperty("project_id"));
        request.setQuery(query);
        return send(request);
    }

    public ShannonDBResultSet getColumns(String catalogName, String schema, String table, Object o)
    {
        return mysqlReader.getColumns(catalogName, schema, table, connectionProperties);
    }

    public ShannonDBPreparedStatement prepareStatement(ShannonDBSocketClient shannonDBSocketClient, String sql)
    {
        ShannonDBPreparedStatement statement = new ShannonDBPreparedStatement(shannonDBSocketClient);
        statement.prepareQuery(sql);
        return statement;
    }

    public void commit()
    {
    }

    public void setAutoCommit(boolean b)
    {
    }

    public boolean isClosed()
    {
        return socket == null || socket.isClosed();
    }

    public void rollback()
    {
    }
}
