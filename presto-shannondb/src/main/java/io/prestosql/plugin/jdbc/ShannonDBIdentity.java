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

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShannonDBIdentity
{
    private final ConnectorSession session;

    public static ShannonDBIdentity from(ConnectorSession session)
    {
        return new ShannonDBIdentity(session.getIdentity().getUser(), session.getIdentity().getExtraCredentials(), session);
    }

    private final String user;
    private final Map<String, String> extraCredentials;

    public ShannonDBIdentity(String user, Map<String, String> extraCredentials, ConnectorSession session)
    {
        this.user = requireNonNull(user, "user is null");
        this.extraCredentials = ImmutableMap.copyOf(requireNonNull(extraCredentials, "extraCredentials is null"));
        this.session = session;
    }

    public String getUser()
    {
        return user;
    }

    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShannonDBIdentity that = (ShannonDBIdentity) o;
        return Objects.equals(user, that.user) &&
                Objects.equals(extraCredentials, that.extraCredentials);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(user, extraCredentials);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("user", user)
                .add("extraCredentials", extraCredentials.keySet())
                .toString();
    }
}
