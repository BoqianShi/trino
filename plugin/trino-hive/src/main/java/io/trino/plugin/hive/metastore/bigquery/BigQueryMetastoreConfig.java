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
package io.trino.plugin.hive.metastore.bigquery;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import java.util.Optional;

public class BigQueryMetastoreConfig
{
    private Optional<String> projectId = Optional.empty();
    private Optional<String> location = Optional.empty(); // e.g., "US", "EU", "us-west1"

    private Optional<String> credentialsKeyPath = Optional.empty();
    private Optional<String> credentialsKeyJson = Optional.empty();

    private Optional<String> defaultWarehouseDir = Optional.empty(); // Useful reference for external tables

    private int bqClientConnectTimeoutMs = 60_000; // Default 60 seconds
    private int bqClientReadTimeoutMs = 60_000;    // Default 60 seconds

    // --- Getters and Setters ---

    public Optional<String> getProjectId()
    {
        return projectId;
    }

    @Config("hive.metastore.bigquery.project-id")
    @ConfigDescription("GCP project ID for BigQuery metastore operations.")
    public BigQueryMetastoreConfig setProjectId(String projectId)
    {
        this.projectId = Optional.ofNullable(projectId);
        return this;
    }

    public Optional<String> getLocation()
    {
        return location;
    }

    @Config("hive.metastore.bigquery.location")
    @ConfigDescription("GCP location (e.g., region or multi-region like 'US') used for BigQuery interactions if required by API.")
    public BigQueryMetastoreConfig setLocation(String location)
    {
        this.location = Optional.ofNullable(location);
        return this;
    }

    public Optional<String> getCredentialsKeyPath()
    {
        return credentialsKeyPath;
    }

    @Config("hive.metastore.bigquery.credentials-key-path")
    @ConfigDescription("Path to Google Cloud service account key file (JSON) for authentication.")
    public BigQueryMetastoreConfig setCredentialsKeyPath(String credentialsKeyPath)
    {
        this.credentialsKeyPath = Optional.ofNullable(credentialsKeyPath);
        return this;
    }

    public Optional<String> getCredentialsKeyJson()
    {
        return credentialsKeyJson;
    }

    @Config("hive.metastore.bigquery.credentials-key-json")
    @ConfigDescription("Google Cloud service account key content (JSON) for authentication.")
    @ConfigSecuritySensitive
    public BigQueryMetastoreConfig setCredentialsKeyJson(String credentialsKeyJson)
    {
        this.credentialsKeyJson = Optional.ofNullable(credentialsKeyJson);
        return this;
    }

    public Optional<String> getDefaultWarehouseDir()
    {
        return defaultWarehouseDir;
    }

    @Config("hive.metastore.bigquery.default-warehouse-dir")
    @ConfigDescription("Default warehouse directory for BigQuery-related operations, potentially for GCS paths if creating external tables referencing GCS.")
    public BigQueryMetastoreConfig setDefaultWarehouseDir(String defaultWarehouseDir)
    {
        this.defaultWarehouseDir = Optional.ofNullable(defaultWarehouseDir);
        return this;
    }

    public int getBqClientConnectTimeoutMs()
    {
        return bqClientConnectTimeoutMs;
    }

    @Config("hive.metastore.bigquery.client.connect-timeout-ms")
    @ConfigDescription("Timeout in milliseconds for connecting to BigQuery service.")
    public BigQueryMetastoreConfig setBqClientConnectTimeoutMs(int bqClientConnectTimeoutMs)
    {
        this.bqClientConnectTimeoutMs = bqClientConnectTimeoutMs;
        return this;
    }

    public int getBqClientReadTimeoutMs()
    {
        return bqClientReadTimeoutMs;
    }

    @Config("hive.metastore.bigquery.client.read-timeout-ms")
    @ConfigDescription("Timeout in milliseconds for reading from BigQuery service.")
    public BigQueryMetastoreConfig setBqClientReadTimeoutMs(int bqClientReadTimeoutMs)
    {
        this.bqClientReadTimeoutMs = bqClientReadTimeoutMs;
        return this;
    }
}
