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
import java.util.Objects;

public class CatalogModel
{
    private final String name;
    private final String driverClassPath;
    private final String sourceType;
    private final List<Parameters> parametersList = new ArrayList<>();
    private final int teamId;

    public CatalogModel(final String name, final String driverClassPath, final String sourceType,
                        final int teamId) {
        this.name = name;
        this.driverClassPath = driverClassPath;
        this.sourceType = sourceType;
        this.teamId = teamId;
    }

    public String getName() {
        return name;
    }

    public String getDriverClassPath() {
        return driverClassPath;
    }

    public String getSourceType() {
        if (sourceType.contains("csv")) {
            return "csv";
        }
        return sourceType;
    }

    public List<Parameters> getParametersList() {
        return parametersList;
    }

    public String getTeamId() {
        return String.valueOf(teamId);
    }

    public void addProperty(final String name, final String value) {
        this.parametersList.add(new Parameters(name, value));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CatalogModel that = (CatalogModel) o;
        return Objects.equals(name, that.name) &&
                teamId == that.teamId &&
                Objects.equals(driverClassPath, that.driverClassPath) &&
                Objects.equals(sourceType, that.sourceType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, driverClassPath, sourceType, parametersList, teamId);
    }

    public static class Parameters {

        private final String configName;

        private final String value;

        public Parameters(final String configName, final String value) {
            this.configName = configName;
            this.value = value;
        }

        public String getConfigName() {
            return configName;
        }

        public String getValue() {
            return value;
        }
    }
}
