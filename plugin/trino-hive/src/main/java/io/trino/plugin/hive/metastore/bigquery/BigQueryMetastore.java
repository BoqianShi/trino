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
import io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

public class BigQueryMetastore
        implements HiveMetastore
{
    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return Optional.empty();
    }

    @Override
    public List<String> getAllDatabases()
    {
        return List.of();
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return Optional.empty();
    }

    @Override
    public Set<HiveColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return Set.of();
    }

    @Override
    public PartitionStatistics getTableStatistics(Table table)
    {
        return null;
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(Table table,
            List<Partition> partitions)
    {
        return Map.of();
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName,
            AcidTransaction transaction,
            Function<PartitionStatistics, PartitionStatistics> update)
    {
    }

    @Override
    public void updatePartitionStatistics(Table table, String partitionName,
            Function<PartitionStatistics, PartitionStatistics> update)
    {
        HiveMetastore.super.updatePartitionStatistics(table, partitionName, update);
    }

    @Override
    public void updatePartitionStatistics(Table table,
            Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        return List.of();
    }

    /**
     * @return List of tables, views and materialized views names from all schemas or Optional.empty
     * if operation is not supported
     */
    @Override
    public Optional<List<SchemaTableName>> getAllTables()
    {
        return Optional.empty();
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey,
            String parameterValue)
    {
        return List.of();
    }

    /**
     * Lists views and materialized views from given database.
     */
    @Override
    public List<String> getAllViews(String databaseName)
    {
        return List.of();
    }

    /**
     * @return List of views including materialized views names from all schemas or Optional.empty
     * if operation is not supported
     */
    @Override
    public Optional<List<SchemaTableName>> getAllViews()
    {
        return Optional.empty();
    }

    @Override
    public void createDatabase(Database database)
    {
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
    }

    /**
     * This should only be used if the semantic here is drop and add. Trying to alter one field of a
     * table object previously acquired from getTable is probably not what you want.
     */
    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable,
            PrincipalPrivileges principalPrivileges)
    {
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName,
            String newTableName)
    {
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName,
            Optional<String> comment)
    {
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName,
            HiveType columnType, String columnComment)
    {
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName,
            String newColumnName)
    {
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return Optional.empty();
    }

    /**
     * Return a list of partition names, with optional filtering (hint to improve performance if
     * possible).
     *
     * @param databaseName the name of the database
     * @param tableName the name of the table
     * @param columnNames the list of partition column names
     * @param partitionKeysFilter optional filter for the partition column values
     * @return a list of partition names as created by {@link MetastoreUtil#toPartitionName}
     * @see TupleDomain
     */
    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName,
            List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return Optional.empty();
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table,
            List<String> partitionNames)
    {
        return Map.of();
    }

    @Override
    public void addPartitions(String databaseName, String tableName,
            List<PartitionWithStatistics> partitions)
    {
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts,
            boolean deleteData)
    {
    }

    @Override
    public void alterPartition(String databaseName, String tableName,
            PartitionWithStatistics partition)
    {
    }

    @Override
    public void createRole(String role, String grantor)
    {
    }

    @Override
    public void dropRole(String role)
    {
    }

    @Override
    public Set<String> listRoles()
    {
        return Set.of();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption,
            HivePrincipal grantor)
    {
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption,
            HivePrincipal grantor)
    {
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
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner,
            HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges,
            boolean grantOption)
    {
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner,
            HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges,
            boolean grantOption)
    {
    }

    /**
     * @param principal when empty, all table privileges are returned
     */
    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName,
            Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return Set.of();
    }

    @Override
    public void checkSupportsTransactions()
    {
        HiveMetastore.super.checkSupportsTransactions();
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
    public void acquireSharedReadLock(AcidTransactionOwner transactionOwner, String queryId,
            long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        HiveMetastore.super.acquireSharedReadLock(transactionOwner, queryId, transactionId,
                fullTables, partitions);
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
    public void acquireTableWriteLock(AcidTransactionOwner transactionOwner, String queryId,
            long transactionId, String dbName, String tableName, DataOperationType operation,
            boolean isDynamicPartitionWrite)
    {
        HiveMetastore.super.acquireTableWriteLock(transactionOwner, queryId, transactionId, dbName,
                tableName, operation, isDynamicPartitionWrite);
    }

    @Override
    public void updateTableWriteId(String dbName, String tableName, long transactionId,
            long writeId, OptionalLong rowCountChange)
    {
        HiveMetastore.super.updateTableWriteId(dbName, tableName, transactionId, writeId,
                rowCountChange);
    }

    @Override
    public void alterPartitions(String dbName, String tableName, List<Partition> partitions,
            long writeId)
    {
        HiveMetastore.super.alterPartitions(dbName, tableName, partitions, writeId);
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames,
            long transactionId, long writeId, AcidOperation operation)
    {
        HiveMetastore.super.addDynamicPartitions(dbName, tableName, partitionNames, transactionId,
                writeId, operation);
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId,
            PrincipalPrivileges principalPrivileges)
    {
        HiveMetastore.super.alterTransactionalTable(table, transactionId, writeId,
                principalPrivileges);
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

    public void createFunction(String databaseName, String functionName,
            LanguageFunction function)
    {
    }

    @Override
    public void replaceFunction(String databaseName, String functionName,
            LanguageFunction function)
    {
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
    }
}
