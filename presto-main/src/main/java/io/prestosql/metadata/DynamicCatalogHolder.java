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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DynamicCatalogHolder
{
    private static List<CatalogModel> loadedCatalogs = new ArrayList<>();

    private DynamicCatalogHolder()
    {
    }

    public static synchronized List<CatalogModel> getLoadedCatalogs()
    {
        return loadedCatalogs;
    }

    public static Optional<CatalogModel> getCatalogByCatalogName(String catalogName)
    {
        return loadedCatalogs.stream().filter(c -> c.getName().equals(catalogName)).findAny();
    }

    public static List<CatalogModel> getCatalogsByTeamId(String teamId)
    {
        return loadedCatalogs.stream().filter(c -> c.getTeamId().equals(teamId)).collect(Collectors.toList());
    }
}
