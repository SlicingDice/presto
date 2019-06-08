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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ShannonDBMysqlReader
{
    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static String URL = "jdbc:mysql://localhost:3306/slicing_dice";

    private static String USER = "root";
    private static String PASS = "";

    public ShannonDBMysqlReader()
    {
        try {
            Class.forName(DRIVER);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setProperties(String url, String user, String password)
    {
        URL = url;
        USER = user;
        PASS = password;
    }

    public ShannonDBResultSet getSchemas(Properties connectionProperties)
    {
        List<Map<String, Object>> list = new ArrayList<>();
        Map<String, Object> schemaMap = new HashMap<>();

        final String schema = connectionProperties.getProperty("team_id") + connectionProperties.getProperty("project_id");

        schemaMap.put("TABLE_SCHEM", schema);
        list.add(schemaMap);

        return new ShannonDBResultSet(list);
    }

    public ShannonDBResultSet getTables(String catalog, String schema, String table, Properties connectionProperties)
    {
        List<Map<String, Object>> list = new ArrayList<>();

        final String sql = "SELECT DISTINCT CONCAT(team_id, '_', project_id, '_', dimension) as table_name  FROM slicing_dice.ProjectField pf JOIN Project p ON p.id = pf.project_id where team_id = ? and project_id = ?";
        try (final Connection conn = DriverManager.getConnection(URL, USER, PASS);
                final PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, connectionProperties.getProperty("team_id"));
            stmt.setString(2, connectionProperties.getProperty("project_id"));
            final ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                Map<String, Object> schemaMap = new HashMap<>();
                schemaMap.put("TABLE_NAME", rs.getString("table_name"));
                schemaMap.put("TABLE_SCHEM", rs.getString("team_id") + rs.getString("project_id"));
                list.add(schemaMap);
            }
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }

        return new ShannonDBResultSet(list);
    }

    public ShannonDBResultSet getColumns(ShannonDBTableHandle tableHandle, Properties connectionProperties)
    {
        List<Map<String, Object>> list = new ArrayList<>();

        final String sql = "SELECT CASE WHEN new_format THEN CONCAT(project_id, '_', dimension, '_', api_name, '_', pf.id) ELSE CONCAT(project_id, '_', dimension, '_', api_name) END as column_name, s1search_type as column_type, CONCAT(team_id, '_', project_id) as group_name, CONCAT(team_id, '_', project_id, '_', dimension) as table_name  FROM slicing_dice.ProjectField pf JOIN Project p ON p.id = pf.project_id where team_id = ? and project_id = ? and table_name = ?";
        try (final Connection conn = DriverManager.getConnection(URL, USER, PASS);
                final PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, connectionProperties.getProperty("team_id"));
            stmt.setString(2, connectionProperties.getProperty("project_id"));
            stmt.setString(3, tableHandle.getTableName());
            final ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                Map<String, Object> schemaMap = new HashMap<>();
                schemaMap.put("DATA_TYPE", rs.getString("column_type"));
                schemaMap.put("TYPE_NAME", rs.getString("column_type"));
                schemaMap.put("COLUMN_NAME", rs.getString("column_name"));
                list.add(schemaMap);
            }
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }

        return new ShannonDBResultSet(list);
    }

    public ShannonDBResultSet getColumns(String catalogName, String schema, String table, Properties connectionProperties)
    {
        List<Map<String, Object>> list = new ArrayList<>();

        final String sql = "SELECT CASE WHEN new_format THEN CONCAT(project_id, '_', dimension, '_', api_name, '_', pf.id) ELSE CONCAT(project_id, '_', dimension, '_', api_name) END as column_name, s1search_type as column_type, CONCAT(team_id, '_', project_id) as group_name, CONCAT(team_id, '_', project_id, '_', dimension) as table_name  FROM slicing_dice.ProjectField pf JOIN Project p ON p.id = pf.project_id where team_id = ? and project_id = ? and table_name = ?";
        try (final Connection conn = DriverManager.getConnection(URL, USER, PASS);
                final PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, connectionProperties.getProperty("team_id"));
            stmt.setString(2, connectionProperties.getProperty("project_id"));
            stmt.setString(3, table);
            final ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                Map<String, Object> schemaMap = new HashMap<>();
                schemaMap.put("DATA_TYPE", rs.getString("column_type"));
                schemaMap.put("TYPE_NAME", rs.getString("column_type"));
                schemaMap.put("COLUMN_NAME", rs.getString("column_name"));
                list.add(schemaMap);
            }
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }

        return new ShannonDBResultSet(list);
    }
}
