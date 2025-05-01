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

import com.google.cloud.bigquery.metastore.client.BigQueryMetastoreClient;
import io.trino.plugin.hive.HiveColumnStatisticType;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.type.Type;
import jakarta.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static java.util.Objects.requireNonNull;

public class BigQueryHiveMetastore
        implements HiveMetastore
{
    // String literals for standard Hive configuration keys
    private static final String HIVE_METASTORE_WAREHOUSE_DIR_KEY = "hive.metastore.warehouse.dir";
    private static final String HIVE_METASTORE_CLIENT_SOCKET_TIMEOUT_KEY = "hive.metastore.client.socket.timeout";
    private static final String HIVE_METASTORE_FS_HANDLER_THREADS_COUNT_KEY = "hive.metastore.fs.handler.threads.count";
    private final BigQueryMetastoreClient bqClient;

    @Inject
    public BigQueryHiveMetastore(BigQueryHiveMetastoreConfig bqConfig) // Only inject your specific config
    {
        requireNonNull(bqConfig, "bqConfig is null");

        Configuration conf = new Configuration(false); // Start with an empty Hadoop Configuration

        // Populate Hadoop Configuration from bqConfig
        conf.set("hive.metastore.bigquery.project.id", bqConfig.getProjectId());
        conf.set("hive.metastore.bigquery.database.location", bqConfig.getLocation());
        conf.set("hive.metastore.bigquery.environment", bqConfig.getEnvironment());
        conf.set("hive.metastore.bigquery.partition.writer.batch.size", String.valueOf(bqConfig.getPartitionWriterBatchSize()));
        conf.set("hive.metastore.bigquery.partition.writer.threads", String.valueOf(bqConfig.getPartitionWriterThreads()));
        conf.set("hive.metastore.bigquery.context.coordinator.threads", String.valueOf(bqConfig.getContextCoordinatorThreads()));

        // Use string literals for standard Hive keys
        conf.setInt(HIVE_METASTORE_FS_HANDLER_THREADS_COUNT_KEY, bqConfig.getFsHandlerThreads());

        if (bqConfig.getClientSocketTimeout() != null) {
            long socketTimeoutMillis = bqConfig.getClientSocketTimeout().toMillis();
            conf.setTimeDuration(
                    HIVE_METASTORE_CLIENT_SOCKET_TIMEOUT_KEY,
                    socketTimeoutMillis,
                    TimeUnit.MILLISECONDS);
        }

        if (bqConfig.getWarehouseDir() != null && !bqConfig.getWarehouseDir().isEmpty()) {
            conf.set(HIVE_METASTORE_WAREHOUSE_DIR_KEY, bqConfig.getWarehouseDir());
        }
        else {
            // Your BigQueryMetastoreClient initializes `new Warehouse(this.conf)`.
            // If METASTOREWAREHOUSE.varname is not set, Warehouse might fail or use a default like "/user/hive/warehouse".
            // It's safer if this is explicitly configured.
            System.err.println("Warning: bq.metastore.warehouse.dir is not configured. " +
                    "BigQueryMetastoreClient might use a default or fail if it's essential.");
        }

        conf.set("metastore.catalog.default", bqConfig.getDefaultCatalog());

        try {
            this.bqClient = new BigQueryMetastoreClient(conf); // Pass the newly populated conf
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to initialize BigQueryMetastoreClient", e);
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return bqClient.getAllDatabases();
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to get all databases from BigQuery Metastore", e);
        }
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        try {
            return bqClient.getAllTables(databaseName);
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to get all tables (Thrift error) for database: " + databaseName, e);
        }
    }

    // --- STUBS for other HiveMetastore interface methods (as in your provided interface) ---
    // (These stubs remain the same as the previous response.)
    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        throw new UnsupportedOperationException("getDatabase not yet implemented");
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        throw new UnsupportedOperationException("getTable not yet implemented");
    }

    @Override
    public Set<HiveColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return Set.of(); // BQ client doesn't support
    }

    @Override
    public PartitionStatistics getTableStatistics(Table table)
    {
        return PartitionStatistics.empty(); // BQ client doesn't support
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions)
    {
        return Map.of(); // BQ client doesn't support
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, AcidTransaction transaction, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException("updateTableStatistics not supported");
    }

    @Override
    public void updatePartitionStatistics(Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        throw new UnsupportedOperationException("updatePartitionStatistics not supported");
    }

    @Override
    public Optional<List<SchemaTableName>> getAllTables()
    { // The no-arg version
        return Optional.empty(); // BQ client doesn't have a direct global equivalent
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        throw new UnsupportedOperationException("getTablesWithParameter not supported");
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        return List.of(); // BQ client doesn't differentiate; could return getAllTables or mark unsupported
    }

    @Override
    public Optional<List<SchemaTableName>> getAllViews()
    { // The no-arg version
        return Optional.empty(); // BQ client doesn't have a direct global equivalent
    }

    @Override
    public void createDatabase(Database database)
    {
        throw new UnsupportedOperationException("createDatabase not supported yet");
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        throw new UnsupportedOperationException("dropDatabase not supported yet");
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        throw new UnsupportedOperationException("renameDatabase not supported");
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        throw new UnsupportedOperationException("setDatabaseOwner not supported");
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException("createTable not supported yet");
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        throw new UnsupportedOperationException("dropTable not supported yet");
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException("replaceTable not supported yet");
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new UnsupportedOperationException("renameTable not supported");
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        throw new UnsupportedOperationException("commentTable not supported");
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        throw new UnsupportedOperationException("setTableOwner not supported");
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        throw new UnsupportedOperationException("commentColumn not supported");
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new UnsupportedOperationException("addColumn not supported");
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new UnsupportedOperationException("renameColumn not supported");
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        throw new UnsupportedOperationException("dropColumn not supported");
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        throw new UnsupportedOperationException("getPartition not supported yet");
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        throw new UnsupportedOperationException("getPartitionNamesByFilter not supported yet");
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        throw new UnsupportedOperationException("getPartitionsByNames not supported yet");
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        throw new UnsupportedOperationException("addPartitions not supported yet");
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        throw new UnsupportedOperationException("dropPartition not supported yet");
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        throw new UnsupportedOperationException("alterPartition not supported yet");
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw new UnsupportedOperationException("createRole not supported by BQ Metastore");
    }

    @Override
    public void dropRole(String role)
    {
        throw new UnsupportedOperationException("dropRole not supported by BQ Metastore");
    }

    @Override
    public Set<String> listRoles()
    {
        throw new UnsupportedOperationException("listRoles not supported by BQ Metastore");
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new UnsupportedOperationException("grantRoles not supported by BQ Metastore");
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new UnsupportedOperationException("revokeRoles not supported by BQ Metastore");
    }

    @Override
    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        throw new UnsupportedOperationException("listGrantedPrincipals not supported by BQ Metastore");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        throw new UnsupportedOperationException("listRoleGrants not supported by BQ Metastore");
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new UnsupportedOperationException("grantTablePrivileges not supported by BQ Metastore");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new UnsupportedOperationException("revokeTablePrivileges not supported by BQ Metastore");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        throw new UnsupportedOperationException("listTablePrivileges not supported by BQ Metastore");
    }

    // ACID methods will use default behavior from HiveMetastore interface (throwing exceptions)

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        return false; // BQ client indicates no UDF support
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName)
    {
        return List.of(); // BQ client indicates no UDF support
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        return List.of(); // BQ client indicates no UDF support
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new UnsupportedOperationException("createFunction not supported by BQ Metastore");
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new UnsupportedOperationException("replaceFunction not supported by BQ Metastore");
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        throw new UnsupportedOperationException("dropFunction not supported by BQ Metastore");
    }
}
