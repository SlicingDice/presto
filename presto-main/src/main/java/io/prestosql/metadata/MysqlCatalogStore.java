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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.connector.ConnectorManager;
import io.prestosql.util.PropertiesUtil;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;

public class MysqlCatalogStore implements CatalogStore
{
    private static final Logger log = Logger.get(MysqlCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    private MysqlCatalogReader mysqlCatalogReader;

    @Inject
    public MysqlCatalogStore(ConnectorManager connectorManager, StaticCatalogStoreConfig config)
    {
        this(connectorManager,
                firstNonNull(config.getDisabledCatalogs(), ImmutableList.of()));
    }

    public MysqlCatalogStore(ConnectorManager connectorManager, List<String> disabledCatalogs)
    {
        this.connectorManager = connectorManager;
        this.disabledCatalogs = ImmutableSet.copyOf(disabledCatalogs);
        this.mysqlCatalogReader = new MysqlCatalogReader();
    }

    public boolean areCatalogsLoaded()
    {
        return catalogsLoaded.get();
    }

    public void loadCatalogs()
            throws Exception
    {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }

        final List<CatalogModel> catalogs = mysqlCatalogReader.loadCatalogs();
        fillCatalogs(catalogs);

        catalogsLoaded.set(true);

        verifyCatalogChanges();
    }

    private void verifyCatalogChanges()
    {
        final int delay = 5;
        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleWithFixedDelay(() ->
        {
            try {
                List<CatalogModel> tempCatalogs = checkNewCatalogs();
                if (!tempCatalogs.isEmpty()) {
                    fillCatalogs(tempCatalogs);
                }
                tempCatalogs = checkExcludedCatalogs();
                if (!tempCatalogs.isEmpty()) {
                    dropCatalogs(tempCatalogs);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }, delay, delay,
                TimeUnit.SECONDS);
    }

    private void dropCatalogs(List<CatalogModel> catalogs)
    {
        for (final CatalogModel catalog : catalogs) {
            connectorManager.dropConnection(catalog.getCatalogName());
        }
        DynamicCatalogHolder.getLoadedCatalogs().removeAll(catalogs);
    }

    private List<CatalogModel> checkExcludedCatalogs()
    {
        final List<CatalogModel> tempCatalogs = mysqlCatalogReader.loadCatalogs();
        final List excluded = new ArrayList(DynamicCatalogHolder.getLoadedCatalogs());
        excluded.removeAll(tempCatalogs);
        return excluded;
    }

    private List<CatalogModel> checkNewCatalogs()
    {
        final List<CatalogModel> tempCatalogs = mysqlCatalogReader.loadCatalogs();
        tempCatalogs.removeAll(DynamicCatalogHolder.getLoadedCatalogs());
        return tempCatalogs;
    }

    private void fillCatalogs(List<CatalogModel> catalogs)
            throws Exception
    {
        for (final CatalogModel model : catalogs) {
            loadCatalog(model);
        }
        DynamicCatalogHolder.getLoadedCatalogs().addAll(catalogs);
    }

    private void loadCatalog(CatalogModel model)
            throws Exception
    {
        String catalogName = model.getCatalogName();
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return;
        }

        log.info("-- Loading catalog %s --", model);

        String connectorName = model.getConnectorName();
        checkState(connectorName != null, "Catalog configuration %s does not contain connector.name", model);

        Map<String, String> properties = new HashMap<>();
        if (!connectorName.equals("shannondb")){
            properties.put("connection-url", model.getUrl());
            properties.put("connection-user", model.getUser());
            properties.put("connection-password", model.getPassword());
        }

        connectorManager.createConnection(catalogName, connectorName, ImmutableMap.copyOf(properties));
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    public void loadProperties(File mysqlConfiguration)
    {
        try {
            final Map<String, String> properties = PropertiesUtil.loadProperties(mysqlConfiguration);
            String url = properties.get("mysql.url");
            String user = properties.get("mysql.user");
            String password = properties.get("mysql.password");
            mysqlCatalogReader.setProperties(url, user, password);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
