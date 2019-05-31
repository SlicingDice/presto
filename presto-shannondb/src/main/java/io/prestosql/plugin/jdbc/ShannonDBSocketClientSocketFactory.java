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

import java.net.Socket;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ShannonDBSocketClientSocketFactory
        implements SocketFactory
{
    private final ShannonDBSocketClient shannonDBSocketClient;
    private final String connectionUrl;
    private final Properties connectionProperties;
    private final Optional<String> userCredentialName;
    private final Optional<String> passwordCredentialName;

    public ShannonDBSocketClientSocketFactory(ShannonDBSocketClient shannonDBSocketClient, BaseShannonDBConfig config)
    {
        this(
                shannonDBSocketClient,
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                basicConnectionProperties(config));
    }

    public static Properties basicConnectionProperties(BaseShannonDBConfig config)
    {
        Properties connectionProperties = new Properties();
        if (config.getConnectionUser() != null) {
            connectionProperties.setProperty("user", config.getConnectionUser());
        }
        if (config.getConnectionPassword() != null) {
            connectionProperties.setProperty("password", config.getConnectionPassword());
        }
        return connectionProperties;
    }

    public ShannonDBSocketClientSocketFactory(ShannonDBSocketClient shannonDBSocketClient, String connectionUrl, Optional<String> userCredentialName, Optional<String> passwordCredentialName, Properties connectionProperties)
    {
        this.shannonDBSocketClient = requireNonNull(shannonDBSocketClient, "shannonDBSocketClient is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "basicConnectionProperties is null"));
        this.userCredentialName = requireNonNull(userCredentialName, "userCredentialName is null");
        this.passwordCredentialName = requireNonNull(passwordCredentialName, "passwordCredentialName is null");
    }

    @Override
    public ShannonDBSocketClient openSocket(ShannonDBIdentity identity)
            throws SQLException
    {
        userCredentialName.ifPresent(credentialName -> setConnectionProperty(connectionProperties, identity.getExtraCredentials(), credentialName, "user"));
        passwordCredentialName.ifPresent(credentialName -> setConnectionProperty(connectionProperties, identity.getExtraCredentials(), credentialName, "password"));

        ShannonDBSocketClient socket = shannonDBSocketClient.connect(connectionUrl, connectionProperties);
        checkState(socket != null, "ShannonDBSocketClient returned null connection");
        return socket;
    }

    private static void setConnectionProperty(Properties connectionProperties, Map<String, String> extraCredentials, String credentialName, String propertyName)
    {
        String value = extraCredentials.get(credentialName);
        if (value != null) {
            connectionProperties.setProperty(propertyName, value);
        }
    }
}
