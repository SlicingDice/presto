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

import javax.inject.Inject;

import java.io.File;

public class CatalogStoreWrapper implements CatalogStore
{
    private static final File MYSQL_CONFIGURATION = new File("etc/mysql.properties");
    private CatalogStore INSTANCE;

    @Inject
    public CatalogStoreWrapper(MysqlCatalogStore mysqlCatalogStore, StaticCatalogStore staticCatalogStore)
    {
        if (MYSQL_CONFIGURATION.exists()) {
            mysqlCatalogStore.loadProperties(MYSQL_CONFIGURATION);
            this.INSTANCE = mysqlCatalogStore;
        }
        else{
            this.INSTANCE = staticCatalogStore;
        }
    }

    @Override
    public void loadCatalogs()
            throws Exception
    {
        this.INSTANCE.loadCatalogs();
    }
}
