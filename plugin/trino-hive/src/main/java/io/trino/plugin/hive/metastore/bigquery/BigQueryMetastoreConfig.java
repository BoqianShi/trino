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
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

// @DefunctConfig({...}) // Add if replacing older configs from a previous BQ implementation
public class BigQueryMetastoreConfig {

    // --- GCP Project & Location ---
    private Optional<String> projectId = Optional.empty();
    private Optional<String> location = Optional.empty(); // e.g., "US", "EU", "us-west1"

    // --- GCP Authentication ---
    private Optional<String> credentialsKeyPath = Optional.empty();
    private Optional<String> credentialsKeyJson = Optional.empty();
    private Optional<String> impersonatedServiceAccount = Optional.empty();

    // --- API Endpoint ---
    private Optional<String> bigqueryEndpointUrl = Optional.empty();

    // --- Client Tuning ---
    // Note: Retries are often handled by the google-http-java-client automatically.
    // Explicit max retries might require custom interceptors if needed beyond defaults.
    private Duration connectTimeout = new Duration(20, TimeUnit.SECONDS);
    private Duration readTimeout = new Duration(60, TimeUnit.SECONDS);
    private int threads = 40; // For parallel operations *within* the metastore implementation logic

    // --- Hive/Metastore Generic ---
    private Optional<String> defaultWarehouseDir = Optional.empty(); // Useful reference for external tables

    // --- Getters and Setters ---

    public Optional<String> getProjectId() {
        return projectId;
    }

    @Config("hive.metastore.bigquery.project-id")
    @ConfigDescription("GCP project ID for BigQuery metastore operations. Auto-detected if running on GCP.")
    public BigQueryMetastoreConfig setProjectId(String projectId) {
        this.projectId = Optional.ofNullable(projectId);
        return this;
    }

    public Optional<String> getLocation() {
        return location;
    }

    @Config("hive.metastore.bigquery.location")
    @ConfigDescription("GCP location (e.g., region or multi-region like 'US') used for BigQuery interactions if required by API.")
    public BigQueryMetastoreConfig setLocation(String location) {
        this.location = Optional.ofNullable(location);
        return this;
    }

    public Optional<String> getCredentialsKeyPath() {
        return credentialsKeyPath;
    }

    @Config("hive.metastore.bigquery.credentials-key-path")
    @ConfigDescription("Path to Google Cloud service account key file (JSON) for authentication.")
    public BigQueryMetastoreConfig setCredentialsKeyPath(String credentialsKeyPath) {
        this.credentialsKeyPath = Optional.ofNullable(credentialsKeyPath);
        return this;
    }

    public Optional<String> getCredentialsKeyJson() {
        return credentialsKeyJson;
    }

    @Config("hive.metastore.bigquery.credentials-key-json")
    @ConfigDescription("Google Cloud service account key content (JSON) for authentication.")
    @ConfigSecuritySensitive
    public BigQueryMetastoreConfig setCredentialsKeyJson(String credentialsKeyJson) {
        this.credentialsKeyJson = Optional.ofNullable(credentialsKeyJson);
        return this;
    }

    public Optional<String> getImpersonatedServiceAccount() {
        return impersonatedServiceAccount;
    }

    @Config("hive.metastore.bigquery.impersonate-service-account")
    @ConfigDescription("Email of Google Cloud service account to impersonate for BigQuery access.")
    public BigQueryMetastoreConfig setImpersonatedServiceAccount(
            String impersonatedServiceAccount) {
        this.impersonatedServiceAccount = Optional.ofNullable(impersonatedServiceAccount);
        return this;
    }

    public Optional<String> getBigqueryEndpointUrl() {
        return bigqueryEndpointUrl;
    }

    @Config("hive.metastore.bigquery.endpoint-url")
    @ConfigDescription("Custom endpoint URL for the BigQuery API.")
    public BigQueryMetastoreConfig setBigqueryEndpointUrl(String bigqueryEndpointUrl) {
        this.bigqueryEndpointUrl = Optional.ofNullable(bigqueryEndpointUrl);
        return this;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    @Config("hive.metastore.bigquery.connect-timeout")
    @ConfigDescription("Connect timeout duration for BigQuery API calls.")
    public BigQueryMetastoreConfig setConnectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public Duration getReadTimeout() {
        return readTimeout;
    }

    @Config("hive.metastore.bigquery.read-timeout")
    @ConfigDescription("Read timeout duration for BigQuery API calls.")
    public BigQueryMetastoreConfig setReadTimeout(Duration readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }

    @Min(1)
    public int getThreads() {
        return threads;
    }

    @Config("hive.metastore.bigquery.threads")
    @ConfigDescription("Number of threads for parallel internal metastore operations (e.g., fetching multiple tables).")
    public BigQueryMetastoreConfig setThreads(int threads) {
        this.threads = threads;
        return this;
    }

    public Optional<String> getDefaultWarehouseDir() {
        return defaultWarehouseDir;
    }

    @Config("hive.metastore.bigquery.default-warehouse-dir")
    @ConfigDescription("Optional base directory path used as reference for external table locations.")
    public BigQueryMetastoreConfig setDefaultWarehouseDir(String defaultWarehouseDir) {
        this.defaultWarehouseDir = Optional.ofNullable(defaultWarehouseDir);
        return this;
    }
}
