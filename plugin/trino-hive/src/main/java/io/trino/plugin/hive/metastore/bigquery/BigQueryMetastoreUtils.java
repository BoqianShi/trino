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

import com.google.api.services.bigquery.model.DatasetReference;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.metastore.Database;
import io.trino.spi.security.PrincipalType;

import java.util.Optional;

public final class BigQueryMetastoreUtils
{
    // Constants previously in BigQueryMetastore
    static final String BIGQUERY_TABLE_FLAG = "bigquery_table";
    static final String BIGQUERY_TABLE_TYPE_PARAMETER = "bigquery_table_type";
    static final String BIGQUERY_TIME_PARTITIONING_FIELD = "bigquery_time_partitioning_field";
    static final String BIGQUERY_TIME_PARTITIONING_TYPE = "bigquery_time_partitioning_type";
    static final String BIGQUERY_CLUSTERING_FIELDS = "bigquery_clustering_fields";

    private BigQueryMetastoreUtils()
    {
        throw new AssertionError("This is a utility class and cannot be instantiated");
    }

    static Database bigQueryApiDatasetToHiveDatabase(com.google.api.services.bigquery.model.Dataset bqDataset)
    {
        if (bqDataset == null) {
            throw new IllegalArgumentException("Input BigQuery Dataset cannot be null");
        }
        if (bqDataset.getDatasetReference() == null || bqDataset.getDatasetReference().getDatasetId() == null) {
            throw new IllegalArgumentException("BigQuery Dataset must have a valid DatasetReference with a DatasetId");
        }

        return Database.builder()
                .setDatabaseName(bqDataset.getDatasetReference().getDatasetId())
                .setLocation(Optional.ofNullable(bqDataset.getLocation()))
                .setComment(Optional.ofNullable(bqDataset.getDescription()))
                .setOwnerName(Optional.of("public")) // Placeholder since BigQuery use IAM to manage ownership
                .setOwnerType(Optional.of(PrincipalType.USER)) // Defaulting
                .setParameters(bqDataset.getLabels() == null ? ImmutableMap.of() : ImmutableMap.copyOf(bqDataset.getLabels()))
                .build();
    }

    static com.google.api.services.bigquery.model.Dataset hiveDatabaseToBigQueryApiModelDataset(
            Database hiveDatabase,
            String projectId,
            String defaultLocation)
    {
        com.google.api.services.bigquery.model.Dataset bqDataset = new com.google.api.services.bigquery.model.Dataset();
        bqDataset.setDatasetReference(new DatasetReference()
                .setProjectId(projectId)
                .setDatasetId(hiveDatabase.getDatabaseName()));
        bqDataset.setLocation(hiveDatabase.getLocation().orElse(defaultLocation));
        hiveDatabase.getComment().ifPresent(bqDataset::setDescription);
        if (hiveDatabase.getParameters() != null && !hiveDatabase.getParameters().isEmpty()) {
            // Filter out any parameters that are not suitable as BQ labels if necessary
            bqDataset.setLabels(ImmutableMap.copyOf(hiveDatabase.getParameters()));
        }
        return bqDataset;
    }
//
//    static Table bigQueryApiTableToHiveTable(com.google.api.services.bigquery.model.Table bqApiTable, String projectIdForTableRef)
//    {
//        TableReference bqTableRef = bqApiTable.getTableReference();
//        if (bqTableRef == null) {
//            throw new IllegalArgumentException("BigQuery API Table must have a TableReference");
//        }
//        // Ensure projectId is populated in the reference for constructing URI, if not already there from API response
//        String actualProjectId = bqTableRef.getProjectId() != null ? bqTableRef.getProjectId() : projectIdForTableRef;
//
//        TableSchema bqSchema = bqApiTable.getSchema();
//
//        List<Column> dataColumns = new ArrayList<>();
//        if (bqSchema != null && bqSchema.getFields() != null) {
//            for (TableFieldSchema field : bqSchema.getFields()) {
//                dataColumns.add(bigQueryFieldSchemaToHiveColumn(field));
//            }
//        }
//
//        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
//        parameters.put(BIGQUERY_TABLE_FLAG, "true");
//        Optional.ofNullable(bqApiTable.getDescription()).ifPresent(comment -> parameters.put("comment", comment));
//        if (bqApiTable.getType() != null) {
//            parameters.put(BIGQUERY_TABLE_TYPE_PARAMETER, bqApiTable.getType());
//        }
//
//
//        TimePartitioning timePartitioning = bqApiTable.getTimePartitioning();
//        if (timePartitioning != null) {
//            Optional.ofNullable(timePartitioning.getField()).ifPresent(field -> parameters.put(BIGQUERY_TIME_PARTITIONING_FIELD, field));
//            Optional.ofNullable(timePartitioning.getType()).ifPresent(type -> parameters.put(BIGQUERY_TIME_PARTITIONING_TYPE, type));
//        }
//        Clustering clustering = bqApiTable.getClustering();
//        if (clustering != null && clustering.getFields() != null && !clustering.getFields().isEmpty()) {
//            parameters.put(BIGQUERY_CLUSTERING_FIELDS, String.join(",", clustering.getFields()));
//        }
//
//        StorageDescriptor sd = StorageDescriptor.builder()
//                .setColumns(dataColumns)
//                .setLocation(Optional.of("bq://" + actualProjectId + "/" + bqTableRef.getDatasetId() + "/" + bqTableRef.getTableId()))
//                .setInputFormat(Optional.empty())
//                .setOutputFormat(Optional.empty())
//                .setSerdeInfo(Optional.empty())
//                .setParameters(parameters.build())
//                .build();
//
//        Table.Builder tableBuilder = Table.builder()
//                .setDatabaseName(bqTableRef.getDatasetId())
//                .setTableName(bqTableRef.getTableId())
//                .setOwner(Optional.empty()) // BigQuery IAM model
//                .setTableType(mapBigQueryApiTableTypeToHiveTableType(bqApiTable.getType()))
//                .setDataColumns(dataColumns)
//                .setPartitionColumns(ImmutableList.of())
//                .setParameters(parameters.build())
//                .setStorageDescriptor(sd);
//
//        if ("VIEW".equals(bqApiTable.getType()) && bqApiTable.getView() != null) {
//            tableBuilder.setViewOriginalText(Optional.ofNullable(bqApiTable.getView().getQuery()));
//            tableBuilder.setViewExpandedText(Optional.ofNullable(bqApiTable.getView().getQuery()));
//        }
//        // TODO: Handle EXTERNAL table properties if bqApiTable.getExternalDataConfiguration() is present
//
//        return tableBuilder.build();
//    }
//
//    static com.google.api.services.bigquery.model.Table hiveTableToBigQueryApiTable(Table hiveTable, String projectId)
//    {
//        com.google.api.services.bigquery.model.Table bqApiTable = new com.google.api.services.bigquery.model.Table();
//        bqApiTable.setTableReference(new TableReference()
//                .setProjectId(projectId)
//                .setDatasetId(hiveTable.getDatabaseName())
//                .setTableId(hiveTable.getTableName()));
//
//        List<TableFieldSchema> bqFields = hiveTable.getDataColumns().stream()
//                .map(BigQueryMetastoreUtils::hiveColumnToBigQueryFieldSchema)
//                .collect(Collectors.toList());
//        bqApiTable.setSchema(new TableSchema().setFields(bqFields));
//
//        hiveTable.getComment().ifPresent(bqApiTable::setDescription);
//
//        Map<String, String> labels = hiveTable.getParameters().entrySet().stream()
//                .filter(entry -> !entry.getKey().startsWith("bigquery_") && !entry.getKey().equals("comment") && !entry.getKey().equals("EXTERNAL"))
//                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//        if (!labels.isEmpty()) {
//            bqApiTable.setLabels(labels);
//        }
//
//        if (VIRTUAL_VIEW.name().equals(hiveTable.getTableType())) {
//            String viewSql = hiveTable.getViewOriginalText()
//                    .orElseThrow(() -> new IllegalArgumentException("View SQL (viewOriginalText) is required for creating a BigQuery view"));
//            bqApiTable.setView(new ViewDefinition().setQuery(viewSql));
//        }
//        else { // Assume TABLE for MANAGED_TABLE, EXTERNAL_TABLE (for now, external needs more)
//            Map<String, String> params = hiveTable.getParameters();
//            if (params.containsKey(BIGQUERY_TIME_PARTITIONING_FIELD) && params.containsKey(BIGQUERY_TIME_PARTITIONING_TYPE)) {
//                bqApiTable.setTimePartitioning(new TimePartitioning()
//                        .setField(params.get(BIGQUERY_TIME_PARTITIONING_FIELD))
//                        .setType(params.get(BIGQUERY_TIME_PARTITIONING_TYPE)));
//            }
//            if (params.containsKey(BIGQUERY_CLUSTERING_FIELDS)) {
//                List<String> clusteringFields = ImmutableList.copyOf(params.get(BIGQUERY_CLUSTERING_FIELDS).split(","));
//                if (!clusteringFields.isEmpty()) {
//                    bqApiTable.setClustering(new Clustering().setFields(clusteringFields));
//                }
//            }
//            // TODO: Handle ExternalDataConfiguration based on parameters or StorageDescriptor
//        }
//        return bqApiTable;
//    }
//
//    static Column bigQueryFieldSchemaToHiveColumn(TableFieldSchema field) {
//        HiveType hiveType = mapBigQueryApiTypeToHiveType(field.getType(), "REPEATED".equals(field.getMode()), field.getFields());
//        return new Column(field.getName(), hiveType, Optional.ofNullable(field.getDescription()), Optional.empty(), ImmutableMap.of());
//    }
//
//    static TableFieldSchema hiveColumnToBigQueryFieldSchema(Column column) {
//        TableFieldSchema fieldSchema = new TableFieldSchema().setName(column.getName());
//        column.getComment().ifPresent(fieldSchema::setDescription);
//
//        String bqType;
//        HiveType.HiveCategory category = column.getType().getHiveCategory();
//
//        if (category == HiveType.HiveCategory.PRIMITIVE) {
//            bqType = mapHivePrimitiveTypeToBigQueryApiType(column.getType());
//            // BigQuery fields are nullable by default unless specified as REQUIRED.
//            // Hive types don't explicitly carry top-level nullability in HiveType for primitives in a simple way.
//            // Trino types do. Assuming NULLABLE as a safe default.
//            fieldSchema.setMode("NULLABLE");
//        }
//        else if (category == HiveType.HiveCategory.LIST) {
//            HiveType elementType = column.getType().getListElementTypeInfo()
//                    .orElseThrow(() -> new TrinoException(HIVE_METASTORE_ERROR, "LIST type has no element type: " + column.getType()))
//                    .toHiveType();
//            bqType = mapHiveTypeToBigQueryApiTypeFull(elementType); // Use a full type mapper for element
//            fieldSchema.setMode("REPEATED");
//        }
//        else if (category == HiveType.HiveCategory.STRUCT) {
//            bqType = "RECORD"; // Or "STRUCT" as synonyms in BQ API string representation
//            fieldSchema.setMode("NULLABLE");
//            List<TableFieldSchema> subFields = column.getType().getStructFieldInfos().stream()
//                    .map(structField -> hiveColumnToBigQueryFieldSchema(new Column(structField.getFieldName(), structField.getFieldHiveType(), structField.getFieldComment())))
//                    .collect(Collectors.toList());
//            fieldSchema.setFields(subFields);
//        }
//        else {
//            throw new UnsupportedOperationException("Unsupported Hive type category for BigQuery conversion: " + category + " for column " + column.getName());
//        }
//        fieldSchema.setType(bqType);
//        return fieldSchema;
//    }
//
//    private static String mapHiveTypeToBigQueryApiTypeFull(HiveType hiveType) {
//        // This dispatches to the correct mapper based on category
//        if (hiveType.getHiveCategory() == HiveType.HiveCategory.PRIMITIVE) {
//            return mapHivePrimitiveTypeToBigQueryApiType(hiveType);
//        }
//        if (hiveType.getHiveCategory() == HiveType.HiveCategory.LIST) {
//            // BQ type for ARRAY is the type of its element, with mode REPEATED
//            return mapHiveTypeToBigQueryApiTypeFull(hiveType.getListElementTypeInfo().get().toHiveType());
//        }
//        if (hiveType.getHiveCategory() == HiveType.HiveCategory.STRUCT) {
//            return "RECORD"; // or "STRUCT"
//        }
//        // MAP and UNION not directly supported by BigQuery native types in this simple mapping
//        throw new UnsupportedOperationException("Cannot map HiveType " + hiveType + " to a singular BigQuery API type string for non-primitive elements directly.");
//    }
//
//    static HiveType mapBigQueryApiTypeToHiveType(String bqApiType, boolean isRepeated, List<TableFieldSchema> subFields) {
//        HiveType singleType;
//        if (bqApiType == null) throw new IllegalArgumentException("BigQuery API type string cannot be null");
//        switch (bqApiType.toUpperCase()) {
//            case "STRING": case "GEOGRAPHY": case "JSON": singleType = HiveType.HIVE_STRING; break;
//            case "INTEGER": case "INT64": singleType = HiveType.HIVE_LONG; break;
//            case "FLOAT": case "FLOAT64": singleType = HiveType.HIVE_DOUBLE; break;
//            case "BOOLEAN": case "BOOL": singleType = HiveType.HIVE_BOOLEAN; break;
//            case "BYTES": singleType = HiveType.HIVE_BINARY; break;
//            case "DATE": singleType = HiveType.HIVE_DATE; break;
//            case "TIMESTAMP": singleType = HiveType.HIVE_TIMESTAMP; break;
//            case "DATETIME": singleType = HiveType.HIVE_TIMESTAMP; break;
//            case "NUMERIC": singleType = HiveType.HIVE_DECIMAL; break;
//            case "BIGNUMERIC": singleType = HiveType.HIVE_DECIMAL; break;
//            case "TIME": singleType = HiveType.HIVE_STRING; break;
//            case "RECORD": case "STRUCT":
//                if (subFields == null) { // An empty struct is possible, but subFields would be an empty list not null
//                    throw new IllegalArgumentException("STRUCT/RECORD type from BigQuery must have sub-fields list (can be empty).");
//                }
//                List<String> fieldNames = subFields.stream().map(TableFieldSchema::getName).collect(Collectors.toList());
//                List<HiveType> fieldTypes = subFields.stream()
//                        .map(sf -> mapBigQueryApiTypeToHiveType(sf.getType(), "REPEATED".equals(sf.getMode()), sf.getFields()))
//                        .collect(Collectors.toList());
//                singleType = HiveType.createStructType(fieldNames, fieldTypes);
//                break;
//            default: throw new UnsupportedOperationException("Unsupported BigQuery API type: " + bqApiType);
//        }
//        return isRepeated ? HiveType.createArrayType(singleType) : singleType;
//    }
//
//    static String mapHivePrimitiveTypeToBigQueryApiType(HiveType hiveType) {
//        String typeName = hiveType.getHiveTypeName().toLowerCase();
//        if (typeName.equals("boolean")) return "BOOLEAN";
//        if (typeName.equals("tinyint") || typeName.equals("smallint") || typeName.equals("int") || typeName.equals("integer") || typeName.equals("bigint")) return "INT64"; // BQ uses INT64 for all integer types
//        if (typeName.equals("float") || typeName.equals("double")) return "FLOAT64";
//        if (typeName.startsWith("decimal")) return "BIGNUMERIC"; // BIGNUMERIC has higher precision and range, safer default
//        if (typeName.startsWith("varchar") || typeName.startsWith("char") || typeName.equals("string")) return "STRING";
//        if (typeName.equals("binary")) return "BYTES";
//        if (typeName.equals("date")) return "DATE";
//        if (typeName.equals("timestamp")) return "TIMESTAMP";
//        throw new UnsupportedOperationException("Unsupported Hive primitive type for BigQuery API conversion: " + typeName);
//    }
//
//    static io.trino.plugin.hive.TableType mapBigQueryApiTableTypeToHiveTableType(String bqApiTableType) {
//        if (bqApiTableType == null) return MANAGED_TABLE;
//        switch (bqApiTableType) {
//            case "TABLE": return MANAGED_TABLE;
//            case "VIEW": return VIRTUAL_VIEW;
//            case "MATERIALIZED_VIEW": return MATERIALIZED_VIEW;
//            case "EXTERNAL": return EXTERNAL_TABLE;
//            case "SNAPSHOT": return MANAGED_TABLE; // Treat snapshot as a regular table for Trino
//            default:
//                // Log warning or throw for unknown types? For now, default to MANAGED_TABLE.
//                return MANAGED_TABLE;
//        }
//    }
}
