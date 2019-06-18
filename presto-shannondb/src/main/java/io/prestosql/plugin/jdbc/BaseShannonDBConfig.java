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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class BaseShannonDBConfig
{
    private String userCredentialName;
    private String passwordCredentialName;
    private String teamId;
    private String projectId;
    private boolean caseInsensitiveNameMatching;
    private Duration caseInsensitiveNameMatchingCacheTtl = new Duration(1, MINUTES);

    @Nullable
    public String getUserCredentialName()
    {
        return userCredentialName;
    }

    @Config("user-credential-name")
    public BaseShannonDBConfig setUserCredentialName(String userCredentialName)
    {
        this.userCredentialName = userCredentialName;
        return this;
    }

    @Nullable
    public String getPasswordCredentialName()
    {
        return passwordCredentialName;
    }

    @Config("password-credential-name")
    public BaseShannonDBConfig setPasswordCredentialName(String passwordCredentialName)
    {
        this.passwordCredentialName = passwordCredentialName;
        return this;
    }

    @Nullable
    public String getTeamId()
    {
        return teamId;
    }

    @Config("team-id")
    public BaseShannonDBConfig setTeamId(String teamId)
    {
        this.teamId = teamId;
        return this;
    }

    @Nullable
    public String getProjectId()
    {
        return projectId;
    }

    @Config("project-id")
    public BaseShannonDBConfig setProjectId(String projectId)
    {
        this.projectId = projectId;
        return this;
    }

    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    @Config("case-insensitive-name-matching")
    public BaseShannonDBConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getCaseInsensitiveNameMatchingCacheTtl()
    {
        return caseInsensitiveNameMatchingCacheTtl;
    }

    @Config("case-insensitive-name-matching.cache-ttl")
    public BaseShannonDBConfig setCaseInsensitiveNameMatchingCacheTtl(Duration caseInsensitiveNameMatchingCacheTtl)
    {
        this.caseInsensitiveNameMatchingCacheTtl = caseInsensitiveNameMatchingCacheTtl;
        return this;
    }
}
