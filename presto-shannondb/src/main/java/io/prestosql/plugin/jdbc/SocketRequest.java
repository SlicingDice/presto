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

public class SocketRequest
{
    private String query;
    private boolean translate_values = true;
    private String project_id;

    public String getProject_id()
    {
        return project_id;
    }

    public String getQuery()
    {
        return query;
    }

    public boolean isTranslate_values()
    {
        return translate_values;
    }

    public void setProject_id(String project_id)
    {
        this.project_id = project_id;
    }

    public void setQuery(String query)
    {
        this.query = query;
    }

    public void setTranslate_values(boolean translate_values)
    {
        this.translate_values = translate_values;
    }


}
