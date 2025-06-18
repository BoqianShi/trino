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
package io.trino.plugin.iceberg.catalog.biglake; // Note the package

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class IcebergBigLakeRestCatalogConfig
{
    private String projectId;
    private String authType;

    public String getProjectId()
    {
        return projectId;
    }

    @Config("iceberg.rest-catalog.biglake.project-id")
    @ConfigDescription("The Google Cloud project ID to use for BigLake")
    public IcebergBigLakeRestCatalogConfig setProjectId(String projectId)
    {
        this.projectId = projectId;
        return this;
    }

    public String getAuthType()
    {
        return authType;
    }

    @Config("iceberg.rest-catalog.rest.auth.type")
    @ConfigDescription("Auth manager class for the rest-catalog")
    public IcebergBigLakeRestCatalogConfig setAuthType(String authType)
    {
        this.authType = authType;
        return this;
    }
}
