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
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.metastore.bigquery;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.TableList;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.hive.thrift.metastore.DataOperationType;
import io.trino.plugin.hive.HiveColumnStatisticType;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidOperation;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.AcidTransactionOwner;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.type.Type;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static java.util.Objects.requireNonNull;

public class BigQueryMetastore
        implements HiveMetastore
{
    private final Bigquery bigQueryClient;
    private final String projectId;
    private final String defaultLocation;
    private final Optional<String> defaultWarehouseDir;

    @Inject
    public BigQueryMetastore(BigQueryMetastoreConfig config)
    {
        requireNonNull(config, "config is null");
        this.projectId = config.getProjectId()
                .orElseThrow(() -> new IllegalArgumentException("Project ID in config is null or not set."));
        this.defaultLocation = config.getLocation()
                .orElseThrow(() -> new IllegalArgumentException("Location in config is null or not set."));
        this.defaultWarehouseDir = config.getDefaultWarehouseDir();

        try {
            HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            JsonFactory jsonFactory = GsonFactory.getDefaultInstance();

            GoogleCredentials credentials;
            if (config.getCredentialsKeyJson().isPresent() && !config.getCredentialsKeyJson().get().isEmpty()) {
                String rawJson = config.getCredentialsKeyJson().get();

                credentials = GoogleCredentials.fromStream(
                                new ByteArrayInputStream(rawJson.getBytes(StandardCharsets.UTF_8)))
                        .createScoped(BigqueryScopes.all());
            }
            else if (config.getCredentialsKeyPath().isPresent() && !config.getCredentialsKeyPath().get().isEmpty()) {
                try (FileInputStream credentialsStream = new FileInputStream(config.getCredentialsKeyPath().get())) {
                    credentials = GoogleCredentials.fromStream(credentialsStream)
                            .createScoped(BigqueryScopes.all());
                }
            }
            else {
                credentials = GoogleCredentials.getApplicationDefault().createScoped(BigqueryScopes.all());
            }

            HttpRequestInitializer httpRequestInitializer = request -> {
                new HttpCredentialsAdapter(credentials).initialize(request);
                request.setConnectTimeout(config.getBqClientConnectTimeoutMs());
                request.setReadTimeout(config.getBqClientReadTimeoutMs());
            };

            this.bigQueryClient = new com.google.api.services.bigquery.Bigquery.Builder(
                    httpTransport,
                    jsonFactory,
                    httpRequestInitializer)
                    .setApplicationName("Trino-BigQueryMetastoreClient/1.0") // Set an appropriate application name
                    .build();
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to initialize BigQuery Service API client (IO): " + e.getMessage(), e);
        }
        catch (GeneralSecurityException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to initialize BigQuery Service API client (Security): " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        try {
            com.google.api.services.bigquery.model.Dataset bqDataset =
                    bigQueryClient.datasets().get(projectId, databaseName).execute();
            return Optional.of(BigQueryMetastoreUtils.bigQueryDatasetToHiveDatabase(bqDataset));
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == 404) {
                return Optional.empty();
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to get BigQuery dataset: " + databaseName + " - " + e.getDetails().getMessage(), e);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to get BigQuery dataset (IO): " + databaseName, e);
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        List<String> databaseNames = new ArrayList<>();
        String nextPageToken = null;
        try {
            do {
                DatasetList datasetListResponse = bigQueryClient.datasets().list(projectId)
                        .setPageToken(nextPageToken)
                        .execute();

                if (datasetListResponse.getDatasets() != null) {
                    for (DatasetList.Datasets datasetListItem : datasetListResponse.getDatasets()) {
                        if (datasetListItem.getDatasetReference() != null) {
                            databaseNames.add(datasetListItem.getDatasetReference().getDatasetId());
                        }
                    }
                }
                nextPageToken = datasetListResponse.getNextPageToken();
            }
            while (nextPageToken != null);
            return databaseNames;
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to list BigQuery datasets", e);
        }
    }

    @Override
    public void createDatabase(Database database)
    {
        try {
            com.google.api.services.bigquery.model.Dataset bqDataset =
                    BigQueryMetastoreUtils.hiveDatabaseToBigQueryModelDataset(database, projectId, defaultLocation);
            bigQueryClient.datasets().insert(projectId, bqDataset).execute();
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == 409) { // Conflict - Already Exists
                throw new TrinoException(HIVE_METASTORE_ERROR, "BigQuery dataset already exists: " + database.getDatabaseName() + " - " + e.getDetails().getMessage(), e);
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to create BigQuery dataset: " + database.getDatabaseName() + " - " + e.getDetails().getMessage(), e);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to create BigQuery dataset (IO): " + database.getDatabaseName(), e);
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        try {
            bigQueryClient.datasets().delete(projectId, databaseName)
                    .setDeleteContents(deleteData)
                    .execute();
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == 404) {
                throw new SchemaNotFoundException(databaseName, "BigQuery dataset not found for deletion: " + databaseName);
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to drop BigQuery dataset: " + databaseName + " - " + e.getDetails().getMessage(), e);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to drop BigQuery dataset (IO): " + databaseName, e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        try {
            com.google.api.services.bigquery.model.Table bqTable =
                    bigQueryClient.tables().get(projectId, databaseName, tableName).execute();
            Optional<Table> hiveTable = Optional.of(BigQueryMetastoreUtils.bigQueryTableToHiveTable(bqTable, projectId));

            return hiveTable;
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == 404) { // Not Found
                return Optional.empty();
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to get BigQuery table: " + databaseName + "." + tableName + " - " + e.getDetails().getMessage(), e);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to get BigQuery table (IO): " + databaseName + "." + tableName, e);
        }
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        if (getDatabase(databaseName).isEmpty()) {
            throw new SchemaNotFoundException(databaseName);
        }

        List<String> tableNames = new ArrayList<>();
        String nextPageToken = null;
        try {
            do {
                TableList tableListResponse = bigQueryClient.tables().list(projectId, databaseName)
                        .setPageToken(nextPageToken)
                        .setMaxResults(1000L)
                        .execute();

                if (tableListResponse.getTables() != null) {
                    for (TableList.Tables tableListItem : tableListResponse.getTables()) {
                        // Filter out types that are not typically considered "tables" by Trino's Hive interface's getAllTables (e.g. SNAPSHOT if BQ adds more)
                        // For now, assume all listed are tables or views that Trino can represent.
                        if (tableListItem.getTableReference() != null &&
                                ("TABLE".equals(tableListItem.getType()) || "VIEW".equals(tableListItem.getType()) || "MATERIALIZED_VIEW".equals(tableListItem.getType()) || "EXTERNAL".equals(tableListItem.getType()))) {
                            tableNames.add(tableListItem.getTableReference().getTableId());
                        }
                    }
                }
                nextPageToken = tableListResponse.getNextPageToken();
            }
            while (nextPageToken != null);
            return tableNames;
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == 404) {
                throw new SchemaNotFoundException(databaseName, "Dataset not found when listing tables: " + e.getDetails().getMessage());
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to list BigQuery tables in dataset: " + databaseName + " - " + e.getDetails().getMessage(), e);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to list BigQuery tables (IO) in dataset: " + databaseName, e);
        }
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
//        if (!table.getPartitionColumns().isEmpty()) {
//            throw new TrinoException(HIVE_METASTORE_ERROR, "Creating Hive-style partitioned tables in BigQuery is not supported by this metastore.");
//        }
//        try {
//            com.google.api.services.bigquery.model.Table bqTable =
//                    BigQueryMetastoreUtils.hiveTableToBigQueryTable(table, projectId);
//            bigQueryClient.tables().insert(projectId, table.getDatabaseName(), bqTable).execute();
//        }
//        catch (GoogleJsonResponseException e) {
//            if (e.getStatusCode() == 409) {
//                throw new TrinoException(HIVE_METASTORE_ERROR, "BigQuery table already exists: " + table.getSchemaTableName() + " - " + e.getDetails().getMessage(), e);
//            }
//            if (e.getStatusCode() == 404) {
//                throw new SchemaNotFoundException(table.getDatabaseName(), "Dataset not found when creating table: " + e.getDetails().getMessage());
//            }
//            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to create BigQuery table: " + table.getSchemaTableName() + " - " + e.getDetails().getMessage(), e);
//        }
//        catch (IOException e) {
//            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to create BigQuery table (IO): " + table.getSchemaTableName(), e);
//        }
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
//        try {
//            bigQueryClient.tables().delete(projectId, databaseName, tableName).execute();
//        }
//        catch (GoogleJsonResponseException e) {
//            if (e.getStatusCode() == 404) {
//                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName), e);
//            }
//            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to drop BigQuery table: " + databaseName + "." + tableName + " - " + e.getDetails().getMessage(), e);
//        }
//        catch (IOException e) {
//            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to drop BigQuery table (IO): " + databaseName + "." + tableName, e);
//        }
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
//        com.google.api.services.bigquery.model.Job job = new com.google.api.services.bigquery.model.Job();
//        com.google.api.services.bigquery.model.JobConfiguration jobConfig = new com.google.api.services.bigquery.model.JobConfiguration();
//        com.google.api.services.bigquery.model.JobConfigurationTableCopy copyConfig = new com.google.api.services.bigquery.model.JobConfigurationTableCopy();
//
//        copyConfig.setSourceTable(new TableReference().setProjectId(projectId).setDatasetId(databaseName).setTableId(tableName));
//        copyConfig.setDestinationTable(new TableReference().setProjectId(projectId).setDatasetId(newDatabaseName).setTableId(newTableName));
//        // Disposition settings might need to align with Trino's RENAME TABLE semantics
//        // CREATE_NEVER would ensure destination does not exist.
//        copyConfig.setCreateDisposition("CREATE_NEVER");
//        copyConfig.setWriteDisposition("WRITE_EMPTY"); // Fails if destination is not empty
//
//        jobConfig.setCopy(copyConfig);
//        job.setConfiguration(jobConfig);
//
//        try {
//            com.google.api.services.bigquery.model.Job executedJob = bigQueryClient.jobs().insert(projectId, job).execute();
//            String jobId = executedJob.getJobReference().getJobId();
//            String jobProjectId = executedJob.getJobReference().getProjectId();
//            // Location might be needed for regional jobs, get it from executedJob.getJobReference().getLocation()
//            String jobLocation = executedJob.getJobReference().getLocation();
//
//            com.google.api.services.bigquery.model.Job completedJob;
//            do {
//                Thread.sleep(1000); // Simple polling, consider exponential backoff or a job utility
//                if (jobLocation != null && !jobLocation.isEmpty()) {
//                    completedJob = bigQueryClient.jobs().get(jobProjectId, jobId).setLocation(jobLocation).execute();
//                }
//                else {
//                    completedJob = bigQueryClient.jobs().get(jobProjectId, jobId).execute();
//                }
//            }
//            while (completedJob.getStatus() != null && !"DONE".equals(completedJob.getStatus().getState()));
//
//            if (completedJob.getStatus() != null && completedJob.getStatus().getErrorResult() != null) {
//                throw new TrinoException(HIVE_METASTORE_ERROR, "Rename (copy) job failed: " + completedJob.getStatus().getErrorResult().getMessage());
//            }
//            if (completedJob.getStatus() == null) {
//                throw new TrinoException(HIVE_METASTORE_ERROR, "Rename (copy) job status was null after completion poll.");
//            }
//
//            dropTable(databaseName, tableName, true); // Delete original table
//        }
//        catch (IOException e) {
//            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to rename (copy) BigQuery table " + databaseName + "." + tableName, e);
//        }
//        catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            throw new TrinoException(HIVE_METASTORE_ERROR, "Rename (copy) operation was interrupted for table " + databaseName + "." + tableName, e);
//        }
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
//        try {
//            com.google.api.services.bigquery.model.Table currentTable = bigQueryClient.tables().get(projectId, databaseName, tableName).execute();
//            // For patch, only send the fields to be updated.
//            com.google.api.services.bigquery.model.Table tablePatch = new com.google.api.services.bigquery.model.Table();
//            tablePatch.setDescription(comment.orElse(null)); // Setting to null will clear the description
//
//            bigQueryClient.tables().patch(projectId, databaseName, tableName, tablePatch).execute();
//        }
//        catch (GoogleJsonResponseException e) {
//            if (e.getStatusCode() == 404) {
//                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName), e);
//            }
//            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to comment BigQuery table: " + databaseName + "." + tableName + " - " + e.getDetails().getMessage(), e);
//        }
//        catch (IOException e) {
//            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to comment BigQuery table (IO): " + databaseName + "." + tableName, e);
//        }
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        if (getDatabase(databaseName).isEmpty()) {
            throw new SchemaNotFoundException(databaseName);
        }
        List<String> viewNames = new ArrayList<>();
        String nextPageToken = null;
        try {
            do {
                TableList tableListResponse = bigQueryClient.tables().list(projectId, databaseName)
                        .setPageToken(nextPageToken)
                        .setMaxResults(1000L)
                        .execute();

                if (tableListResponse.getTables() != null) {
                    for (TableList.Tables tableListItem : tableListResponse.getTables()) {
                        if (tableListItem.getTableReference() != null &&
                                ("VIEW".equals(tableListItem.getType()) || "MATERIALIZED_VIEW".equals(tableListItem.getType()))) {
                            viewNames.add(tableListItem.getTableReference().getTableId());
                        }
                    }
                }
                nextPageToken = tableListResponse.getNextPageToken();
            }
            while (nextPageToken != null);
            return viewNames;
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == 404) {
                throw new SchemaNotFoundException(databaseName, "Dataset not found when listing views: " + e.getDetails().getMessage());
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to list BigQuery views in dataset: " + databaseName + " - " + e.getDetails().getMessage(), e);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to list BigQuery views (IO) in dataset: " + databaseName, e);
        }
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
//        try {
//            dropTable(databaseName, tableName, true);
//        }
//        catch (TableNotFoundException e) {
//            // It's okay if the table doesn't exist when trying to replace it.
//        }
//        createTable(newTable, principalPrivileges);
    }

    // --- All other methods remain as they were (stubs or default implementations) ---
    // ... (rest of the methods from your template) ...

    @Override
    public Set<HiveColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return ImmutableSet.of();
    }

    @Override
    public PartitionStatistics getTableStatistics(Table table)
    {
        return PartitionStatistics.empty();
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions)
    {
        return ImmutableMap.of();
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, AcidTransaction transaction, Function<PartitionStatistics, PartitionStatistics> update)
    {
    }

    @Override
    public void updatePartitionStatistics(Table table, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        HiveMetastore.super.updatePartitionStatistics(table, partitionName, update);
    }

    @Override
    public void updatePartitionStatistics(Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
    }

    @Override
    public Optional<List<SchemaTableName>> getAllTables()
    {
        return Optional.empty();
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        return List.of();
    }

    @Override
    public Optional<List<SchemaTableName>> getAllViews()
    {
        return Optional.empty();
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        throw new UnsupportedOperationException("Renaming BigQuery dataset (database) is not directly supported by this metastore.");
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        throw new UnsupportedOperationException("Setting BigQuery dataset owner directly is not supported. Manage permissions via GCP IAM.");
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        throw new UnsupportedOperationException("Setting BigQuery table owner directly is not supported. Manage permissions via GCP IAM.");
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        throw new UnsupportedOperationException("Commenting on a BigQuery table column requires schema update, not supported in this basic version.");
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new UnsupportedOperationException("Adding a column to a BigQuery table requires schema update, not supported in this basic version.");
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new UnsupportedOperationException("Renaming a column in a BigQuery table requires schema update, not supported in this basic version.");
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        throw new UnsupportedOperationException("Dropping a column from a BigQuery table requires schema update, not supported in this basic version.");
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return Optional.empty();
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return Optional.empty();
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        return Map.of();
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        throw new UnsupportedOperationException("Hive-style partitioning not supported.");
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        throw new UnsupportedOperationException("Hive-style partitioning not supported.");
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        throw new UnsupportedOperationException("Hive-style partitioning not supported.");
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropRole(String role)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listRoles()
    {
        return Set.of();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        return Set.of();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return Set.of();
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return Set.of();
    }

    @Override
    public void checkSupportsTransactions()
    {
        throw new UnsupportedOperationException("Hive ACID transactions are not supported by this BigQuery metastore.");
    }

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner)
    {
        return HiveMetastore.super.openTransaction(transactionOwner);
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        HiveMetastore.super.commitTransaction(transactionId);
    }

    @Override
    public void abortTransaction(long transactionId)
    {
        HiveMetastore.super.abortTransaction(transactionId);
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        HiveMetastore.super.sendTransactionHeartbeat(transactionId);
    }

    @Override
    public void acquireSharedReadLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        HiveMetastore.super.acquireSharedReadLock(transactionOwner, queryId, transactionId, fullTables, partitions);
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        return HiveMetastore.super.getValidWriteIds(tables, currentTransactionId);
    }

    @Override
    public Optional<String> getConfigValue(String name)
    {
        return HiveMetastore.super.getConfigValue(name);
    }

    @Override
    public long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        return HiveMetastore.super.allocateWriteId(dbName, tableName, transactionId);
    }

    @Override
    public void acquireTableWriteLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, String dbName, String tableName, DataOperationType operation, boolean isDynamicPartitionWrite)
    {
        HiveMetastore.super.acquireTableWriteLock(transactionOwner, queryId, transactionId, dbName, tableName, operation, isDynamicPartitionWrite);
    }

    @Override
    public void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        HiveMetastore.super.updateTableWriteId(dbName, tableName, transactionId, writeId, rowCountChange);
    }

    @Override
    public void alterPartitions(String dbName, String tableName, List<Partition> partitions, long writeId)
    {
        HiveMetastore.super.alterPartitions(dbName, tableName, partitions, writeId);
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        HiveMetastore.super.addDynamicPartitions(dbName, tableName, partitionNames, transactionId, writeId, operation);
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId, PrincipalPrivileges principalPrivileges)
    {
        HiveMetastore.super.alterTransactionalTable(table, transactionId, writeId, principalPrivileges);
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        return false;
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName)
    {
        return List.of();
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        return List.of();
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        throw new UnsupportedOperationException();
    }
}
