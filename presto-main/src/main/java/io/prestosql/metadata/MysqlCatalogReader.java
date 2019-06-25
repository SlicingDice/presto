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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
        final Map<String, CatalogModel> catalogs = new ConcurrentHashMap<>();
        try (final Connection conn = DriverManager.getConnection(URL, USER, PASS);
                final Statement stmt = conn.createStatement()) {
            final String sql = "SELECT ds.id, driver_class_path, source_type, dsc.name, dsc.value," +
                    " ds.team_id, (SELECT value FROM DataSourceConfig dsci WHERE dsci.name = 'projects' AND dsci.id_data_source = ds.id) AS projects " +
                    "FROM DataSource ds " +
                    "JOIN DataSourceConfig dsc ON ds.id = dsc.id_data_source " +
                    "WHERE source_usage IN (2, 3) AND NOT deleted AND NOT dsc.ignore AND source_type is not null AND source_type != '';";
            final ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                final String driverClassPath = rs.getString("driver_class_path");
                final String sourceType = rs.getString("source_type");
                final String name = rs.getString("name");
                final String value = rs.getString("value");
                final int teamId = rs.getInt("team_id");
                String projects = rs.getString("projects");

                if (projects != null) {
                    projects = projects.replace("[", "").replace("]", "");
                    if (projects.length() > 0) {
                        for (final String s : projects.split(",")) {
                            final String catalogName = String.format("%s_%s", teamId, s.trim());

                            final CatalogModel catalogModel = catalogs.computeIfAbsent(catalogName,
                                    k -> new CatalogModel(catalogName, driverClassPath, sourceType, teamId));

                            catalogModel.addProperty(name, value);
                        }
                    } else {
                        final CatalogModel catalogModel = catalogs.computeIfAbsent(String.valueOf(teamId),
                                k -> new CatalogModel(String.valueOf(teamId), driverClassPath, sourceType, teamId));

                        catalogModel.addProperty(name, value);
                    }
                } else {
                    final CatalogModel catalogModel = catalogs.computeIfAbsent(String.valueOf(teamId),
                            k -> new CatalogModel(String.valueOf(teamId), driverClassPath, sourceType, teamId));

                    catalogModel.addProperty(name, value);
                }
            }
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        return new ArrayList<>(catalogs.values());
    }


    public void setProperties(String url, String user, String password)
    {
        URL = url;
        USER = user;
        PASS = password;
    }
}
