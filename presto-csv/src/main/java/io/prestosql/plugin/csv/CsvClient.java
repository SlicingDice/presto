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
package io.prestosql.plugin.csv;

import com.slicingdice.drivers.SlicingDiceCsvDriver;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;

import javax.inject.Inject;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static io.prestosql.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;

public class CsvClient
        extends BaseJdbcClient
{
    protected final Type jsonType;

    @Inject
    public CsvClient(BaseJdbcConfig config, TypeManager typeManager)
            throws SQLException
    {
        super(config, "\"", connectionFactory(config));
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);

        return new DriverConnectionFactory(new SlicingDiceCsvDriver(), config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                connectionProperties);
    }
}
