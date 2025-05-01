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
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;

import java.util.concurrent.TimeUnit;

public class BigQueryHiveMetastoreConfig
{
    // Properties directly for BigQueryMetastoreClient's custom keys
    private String projectId;
    private String location;
    private String environment = "PROD"; // Default as seen in BigQueryMetastoreClient's usage
    private int partitionWriterBatchSize = 900; // Default from BigQueryMetastoreClient
    private int partitionWriterThreads = 2;     // Default from BigQueryMetastoreClient
    private int contextCoordinatorThreads = 6;  // Default from BigQueryMetastoreClient
    private String defaultCatalog = "hive";     // Default from BigQueryMetastoreClient logic

    // Properties to replace those usually from Trino's MetastoreConfig/HiveConfig
    // These will be used to set standard Hive conf keys
    private String warehouseDir; // For hive.metastore.warehouse.dir
    private Duration clientSocketTimeout = new Duration(60, TimeUnit.SECONDS); // A sensible default, similar to Hive's
    private int fsHandlerThreads = 15; // Default from HiveConf.ConfVars.METASTORE_FS_HANDLER_THREADS_COUNT

    @NotEmpty(message = "hive.metastore.bigquery.project-id is required")
    public String getProjectId()
    {
        return projectId;
    }

    @Config("hive.metastore.bigquery.project-id")
    @ConfigDescription("GCP project ID for BigQuery metastore operations.")
    public BigQueryHiveMetastoreConfig setProjectId(String projectId)
    {
        this.projectId = projectId;
        return this;
    }

    @NotEmpty(message = "hive.metastore.bigquery.location is required")
    public String getLocation()
    {
        return location;
    }

    @Config("hive.metastore.bigquery.location")
    public BigQueryHiveMetastoreConfig setLocation(String location)
    {
        this.location = location;
        return this;
    }

    public String getEnvironment()
    {
        return environment;
    }

    @Config("hive.metastore.bigquery.environment")
    public BigQueryHiveMetastoreConfig setEnvironment(String environment)
    {
        this.environment = environment;
        return this;
    }

    @Min(1)
    public int getPartitionWriterBatchSize()
    {
        return partitionWriterBatchSize;
    }

    @Config("hive.metastore.bigquery.partition.writer.batch-size")
    public BigQueryHiveMetastoreConfig setPartitionWriterBatchSize(int partitionWriterBatchSize)
    {
        this.partitionWriterBatchSize = partitionWriterBatchSize;
        return this;
    }

    @Min(1)
    public int getPartitionWriterThreads()
    {
        return partitionWriterThreads;
    }

    @Config("hive.metastore.bigquery.partition.writer.threads")
    public BigQueryHiveMetastoreConfig setPartitionWriterThreads(int partitionWriterThreads)
    {
        this.partitionWriterThreads = partitionWriterThreads;
        return this;
    }

    @Min(1)
    public int getContextCoordinatorThreads()
    {
        return contextCoordinatorThreads;
    }

    @Config("hive.metastore.bigquery.context-coordinator-threads")
    public BigQueryHiveMetastoreConfig setContextCoordinatorThreads(int contextCoordinatorThreads)
    {
        this.contextCoordinatorThreads = contextCoordinatorThreads;
        return this;
    }

    @NotEmpty
    public String getDefaultCatalog()
    {
        return defaultCatalog;
    }

    @Config("hive.metastore.bigquery.default-catalog")
    public BigQueryHiveMetastoreConfig setDefaultCatalog(String defaultCatalog)
    {
        this.defaultCatalog = defaultCatalog;
        return this;
    }

    // This now comes from this config class directly
    public String getWarehouseDir()
    {
        return warehouseDir;
    }

    @Config("hive.metastore.bigquery.warehouse.dir") // Trino config key
    public BigQueryHiveMetastoreConfig setWarehouseDir(String warehouseDir)
    {
        this.warehouseDir = warehouseDir;
        return this;
    }

    // This now comes from this config class directly
    public Duration getClientSocketTimeout()
    {
        return clientSocketTimeout;
    }

    @Config("hive.metastore.bigquery.client.socket-timeout") // Trino config key
    public BigQueryHiveMetastoreConfig setClientSocketTimeout(Duration clientSocketTimeout)
    {
        this.clientSocketTimeout = clientSocketTimeout;
        return this;
    }

    // This now comes from this config class directly
    @Min(1)
    public int getFsHandlerThreads()
    {
        return fsHandlerThreads;
    }

    @Config("hive.metastore.bigquery.fs-handler-threads") // Trino config key
    public BigQueryHiveMetastoreConfig setFsHandlerThreads(int fsHandlerThreads)
    {
        this.fsHandlerThreads = fsHandlerThreads;
        return this;
    }
}
