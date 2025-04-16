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

import com.google.common.collect.ImmutableSet;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.AcidTransactionOwner;
import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

public class BigQueryMetastore
        implements HiveMetastore {

    @Override
    public Optional<Database> getDatabase(String databaseName) {
        return Optional.empty();
    }

    @Override
    public List<String> getAllDatabases() {
        return List.of();
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName) {
        return Optional.empty();
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName,
            String tableName, Set<String> columnNames) {
        return Map.of();
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(
            String databaseName, String tableName, Set<String> partitionNames,
            Set<String> columnNames) {
        return Map.of();
    }

    @Override
    public boolean useSparkTableStatistics() {
        return HiveMetastore.super.useSparkTableStatistics();
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName,
            OptionalLong acidWriteId,
            StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate) {

    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode,
            Map<String, PartitionStatistics> partitionUpdates) {

    }

    @Override
    public List<TableInfo> getTables(String databaseName) {
        return List.of();
    }

    @Override
    public List<String> getTableNamesWithParameters(String databaseName, String parameterKey,
            ImmutableSet<String> parameterValues) {
        return List.of();
    }

    @Override
    public void createDatabase(Database database) {

    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData) {

    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName) {

    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal) {

    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges) {

    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData) {

    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable,
            PrincipalPrivileges principalPrivileges) {

    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName,
            String newTableName) {

    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment) {

    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal) {

    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName,
            Optional<String> comment) {

    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName,
            HiveType columnType, String columnComment) {

    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName,
            String newColumnName) {

    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName) {

    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues) {
        return Optional.empty();
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName,
            List<String> columnNames, TupleDomain<String> partitionKeysFilter) {
        return Optional.empty();
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table,
            List<String> partitionNames) {
        return Map.of();
    }

    @Override
    public void addPartitions(String databaseName, String tableName,
            List<PartitionWithStatistics> partitions) {

    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts,
            boolean deleteData) {

    }

    @Override
    public void alterPartition(String databaseName, String tableName,
            PartitionWithStatistics partition) {

    }

    @Override
    public void createRole(String role, String grantor) {

    }

    @Override
    public void dropRole(String role) {

    }

    @Override
    public Set<String> listRoles() {
        return Set.of();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption,
            HivePrincipal grantor) {

    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption,
            HivePrincipal grantor) {

    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal) {
        return Set.of();
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner,
            HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges,
            boolean grantOption) {

    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner,
            HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges,
            boolean grantOption) {

    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName,
            Optional<String> tableOwner, Optional<HivePrincipal> principal) {
        return Set.of();
    }

    @Override
    public void checkSupportsTransactions() {
        HiveMetastore.super.checkSupportsTransactions();
    }

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner) {
        return HiveMetastore.super.openTransaction(transactionOwner);
    }

    @Override
    public void commitTransaction(long transactionId) {
        HiveMetastore.super.commitTransaction(transactionId);
    }

    @Override
    public void abortTransaction(long transactionId) {
        HiveMetastore.super.abortTransaction(transactionId);
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId) {
        HiveMetastore.super.sendTransactionHeartbeat(transactionId);
    }

    @Override
    public void acquireSharedReadLock(AcidTransactionOwner transactionOwner, String queryId,
            long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions) {
        HiveMetastore.super.acquireSharedReadLock(transactionOwner, queryId, transactionId,
                fullTables,
                partitions);
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId) {
        return HiveMetastore.super.getValidWriteIds(tables, currentTransactionId);
    }

    @Override
    public Optional<String> getConfigValue(String name) {
        return HiveMetastore.super.getConfigValue(name);
    }

    @Override
    public long allocateWriteId(String dbName, String tableName, long transactionId) {
        return HiveMetastore.super.allocateWriteId(dbName, tableName, transactionId);
    }

    @Override
    public void acquireTableWriteLock(AcidTransactionOwner transactionOwner, String queryId,
            long transactionId, String dbName, String tableName, AcidOperation operation,
            boolean isDynamicPartitionWrite) {
        HiveMetastore.super.acquireTableWriteLock(transactionOwner, queryId, transactionId, dbName,
                tableName, operation, isDynamicPartitionWrite);
    }

    @Override
    public void updateTableWriteId(String dbName, String tableName, long transactionId,
            long writeId,
            OptionalLong rowCountChange) {
        HiveMetastore.super.updateTableWriteId(dbName, tableName, transactionId, writeId,
                rowCountChange);
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames,
            long transactionId, long writeId, AcidOperation operation) {
        HiveMetastore.super.addDynamicPartitions(dbName, tableName, partitionNames, transactionId,
                writeId, operation);
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId,
            PrincipalPrivileges principalPrivileges) {
        HiveMetastore.super.alterTransactionalTable(table, transactionId, writeId,
                principalPrivileges);
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken) {
        return false;
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName) {
        return List.of();
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName) {
        return List.of();
    }

    @Override
    public void createFunction(String databaseName, String functionName,
            LanguageFunction function) {

    }

    @Override
    public void replaceFunction(String databaseName, String functionName,
            LanguageFunction function) {

    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken) {

    }
}
