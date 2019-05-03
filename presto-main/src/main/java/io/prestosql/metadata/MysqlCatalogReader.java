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
package io.prestosql.metadata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class MysqlCatalogReader
{
    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static String URL;

    private static String USER;
    private static String PASS;

    public MysqlCatalogReader()
    {
        try {
            Class.forName(DRIVER);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<CatalogModel> loadCatalogs()
    {
        final List<CatalogModel> catalogs = new ArrayList<>();
        try (final Connection conn = DriverManager.getConnection(URL, USER, PASS);
                final Statement stmt = conn.createStatement()) {
            final String sql = "select * from catalog";
            final ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                CatalogModel catalog = new CatalogModel(rs.getInt("id"), rs.getString("connector_name"), rs.getString("catalog_name"), rs.getString("url"), rs.getString("user"), rs.getString("password"), rs.getString("owner"));
                catalogs.add(catalog);
            }
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return catalogs;
    }


    public void setProperties(String url, String user, String password)
    {
        URL = url;
        USER = user;
        PASS = password;
    }
}
