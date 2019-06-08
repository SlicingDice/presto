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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.procedure.Procedure;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class ShannonDBModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        newOptionalBinder(binder, ConnectorAccessControl.class);
        newSetBinder(binder, Procedure.class);
        binder.bind(ShannonDBClient.class).to(BaseShannonDBClient.class).in(Scopes.SINGLETON);
        binder.bind(ShannonDBMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(BaseShannonDBConfig.class).in(Scopes.SINGLETON);
        binder.bind(ShannonDBSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ShannonDBRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ShannonDBPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ShannonDBConnector.class).in(Scopes.SINGLETON);
        binder.bind(SocketFactory.class).to(ShannonDBSocketClientSocketFactory.class).in(Scopes.SINGLETON);
        binder.bind(ShannonDBSocketClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ShannonDBMetadataConfig.class);
    }
}
