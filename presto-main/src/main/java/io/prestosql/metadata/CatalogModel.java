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

public class CatalogModel
{
    private int id;
    private String catalogName;
    private String connectorName;
    private String url;
    private String user;
    private String password;
    private String owner;

    public CatalogModel(int id, String connectorName, String catalogName, String url, String user, String password, String owner)
    {
        this.id = id;
        this.connectorName = connectorName;
        this.catalogName = catalogName;
        this.url = url;
        this.user = user;
        this.password = password;
        this.owner = owner;
    }

    public int getId()
    {
        return id;
    }

    public String getConnectorName()
    {
        return connectorName;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getPassword()
    {
        return password;
    }

    public String getUrl()
    {
        return url;
    }

    public String getUser()
    {
        return user;
    }

    public String getOwner()
    {
        return owner;
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

        CatalogModel that = (CatalogModel) o;

        if (id != that.id) {
            return false;
        }
        if (catalogName != null ? !catalogName.equals(that.catalogName) : that.catalogName != null) {
            return false;
        }
        if (connectorName != null ? !connectorName.equals(that.connectorName) : that.connectorName != null) {
            return false;
        }
        if (url != null ? !url.equals(that.url) : that.url != null) {
            return false;
        }
        if (user != null ? !user.equals(that.user) : that.user != null) {
            return false;
        }
        return password != null ? password.equals(that.password) : that.password == null;
    }

    @Override
    public int hashCode()
    {
        int result = id;
        result = 31 * result + (catalogName != null ? catalogName.hashCode() : 0);
        result = 31 * result + (connectorName != null ? connectorName.hashCode() : 0);
        result = 31 * result + (url != null ? url.hashCode() : 0);
        result = 31 * result + (user != null ? user.hashCode() : 0);
        result = 31 * result + (password != null ? password.hashCode() : 0);
        return result;
    }
}
