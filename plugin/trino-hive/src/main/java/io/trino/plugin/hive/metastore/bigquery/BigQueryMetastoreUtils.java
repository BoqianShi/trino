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

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.CsvOptions;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ExternalDataConfiguration;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.api.services.bigquery.model.ViewDefinition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.type.Category;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.security.PrincipalType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;

public final class BigQueryMetastoreUtils
{
    // Constants (Unchanged)
    static final String BIGQUERY_TABLE_FLAG = "bigquery_table";
    static final String BIGQUERY_TABLE_TYPE_PARAMETER = "bigquery_table_type";
    static final String BIGQUERY_TIME_PARTITIONING_FIELD = "bigquery_time_partitioning_field";
    static final String BIGQUERY_TIME_PARTITIONING_TYPE = "bigquery_time_partitioning_type";
    static final String BIGQUERY_CLUSTERING_FIELDS = "bigquery_clustering_fields";
    static final String CSV_SKIP_HEADER_LINE_COUNT = "skip.header.line.count";
    static final String EXTERNAL_DATA_FORMAT = "external_data_format";
    static final String EXTERNAL_DATA_COMPRESSION = "external_data_compression";
    private static final String FIELD_DELIM = "field.delim";
    private static final String SERIALIZATION_FORMAT = "serialization.format";
    private static final String ESCAPE_CHAR = "escape.delim";
    private static final String QUOTE_CHAR = "quote.delim";
    private static final String SERIALIZATION_ENCODING = "serialization.encoding";

    private BigQueryMetastoreUtils() {}

    // --- Dataset Conversion (Unchanged) ---
    static Database bigQueryDatasetToHiveDatabase(Dataset bqDataset)
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
                .setOwnerName(Optional.of("public"))  // Placeholder since BigQuery use IAM to manage ownership
                .setOwnerType(Optional.of(PrincipalType.USER))
                .setParameters(bqDataset.getLabels() == null ? ImmutableMap.of() : ImmutableMap.copyOf(bqDataset.getLabels()))
                .build();
    }

    static Dataset hiveDatabaseToBigQueryModelDataset(Database hiveDatabase, String projectId, String defaultLocation)
    {
        Dataset bqDataset = new Dataset();
        bqDataset.setDatasetReference(new DatasetReference()
                .setProjectId(projectId)
                .setDatasetId(hiveDatabase.getDatabaseName()));
        bqDataset.setLocation(hiveDatabase.getLocation().orElse(defaultLocation));
        hiveDatabase.getComment().ifPresent(bqDataset::setDescription);
        if (hiveDatabase.getParameters() != null && !hiveDatabase.getParameters().isEmpty()) {
            bqDataset.setLabels(ImmutableMap.copyOf(hiveDatabase.getParameters()));
        }
        return bqDataset;
    }

    static Table bigQueryTableToHiveTable(com.google.api.services.bigquery.model.Table bqTable, String projectIdForTableRef)
    {
        TableReference bqTableRef = bqTable.getTableReference();
        if (bqTableRef == null) {
            throw new IllegalArgumentException("BigQuery API Table must have a TableReference");
        }
        String actualProjectId = bqTableRef.getProjectId() != null ? bqTableRef.getProjectId() : projectIdForTableRef;
        String datasetId = bqTableRef.getDatasetId();
        String tableId = bqTableRef.getTableId();

        TableSchema bqSchema = bqTable.getSchema();
        List<Column> dataColumns = new ArrayList<>();
        if (bqSchema != null && bqSchema.getFields() != null) {
            for (TableFieldSchema field : bqSchema.getFields()) {
                dataColumns.add(bigQueryFieldSchemaToHiveColumn(field));
            }
        }

        ImmutableMap.Builder<String, String> tableParameters = ImmutableMap.builder();
        tableParameters.put(BIGQUERY_TABLE_FLAG, "true");
        Optional.ofNullable(bqTable.getDescription()).ifPresent(comment -> tableParameters.put("comment", comment));
        if (bqTable.getType() != null) {
            tableParameters.put(BIGQUERY_TABLE_TYPE_PARAMETER, bqTable.getType());
        }

        TimePartitioning timePartitioning = bqTable.getTimePartitioning();
        if (timePartitioning != null) {
            Optional.ofNullable(timePartitioning.getField()).ifPresent(field -> tableParameters.put(BIGQUERY_TIME_PARTITIONING_FIELD, field));
            Optional.ofNullable(timePartitioning.getType()).ifPresent(type -> tableParameters.put(BIGQUERY_TIME_PARTITIONING_TYPE, type));
        }
        Clustering clustering = bqTable.getClustering();
        if (clustering != null && clustering.getFields() != null && !clustering.getFields().isEmpty()) {
            tableParameters.put(BIGQUERY_CLUSTERING_FIELDS, String.join(",", clustering.getFields()));
        }

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(datasetId)
                .setTableName(tableId)
                .setOwner(Optional.empty())
                .setTableType(bqTable.getType())
                .setDataColumns(dataColumns)
                .setPartitionColumns(ImmutableList.of());

        tableBuilder.withStorage(storageBuilder -> {
            storageBuilder.setSkewed(false);
            storageBuilder.setBucketProperty(Optional.empty());

            Map<String, String> serdeParameters = new HashMap<>();
            String serdeLib = null;
            String inputFormat = null;
            String outputFormat = null;

            if (TableType.isBigQueryExternalTable(bqTable)) {
                ExternalDataConfiguration externalConfig = bqTable.getExternalDataConfiguration();
                if (externalConfig.getSourceUris() != null && !externalConfig.getSourceUris().isEmpty()) {
                    storageBuilder.setLocation(externalConfig.getSourceUris().get(0));
                    if (externalConfig.getSourceUris().size() > 1) {
                        tableParameters.put("external.source.uris", String.join(",", externalConfig.getSourceUris()));
                    }
                }
                else {
                    storageBuilder.setLocation(Optional.empty());
                }
                String format = externalConfig.getSourceFormat();
                if (format != null) {
                    tableParameters.put(EXTERNAL_DATA_FORMAT, format);
                    switch (format) {
                        case "CSV":
                            inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
                            outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
                            serdeLib = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
                            if (externalConfig.getCsvOptions() != null) {
                                CsvOptions csvOptions = externalConfig.getCsvOptions();
                                if (csvOptions.getSkipLeadingRows() != null && csvOptions.getSkipLeadingRows() > 0) {
                                    tableParameters.put(CSV_SKIP_HEADER_LINE_COUNT, csvOptions.getSkipLeadingRows().toString());
                                }
                                Optional.ofNullable(csvOptions.getFieldDelimiter()).ifPresent(d -> serdeParameters.put(FIELD_DELIM, d));
                                Optional.ofNullable(csvOptions.getQuote()).ifPresent(q -> serdeParameters.put(QUOTE_CHAR, q));
                                Optional.ofNullable(csvOptions.getEncoding()).ifPresent(e -> serdeParameters.put(SERIALIZATION_ENCODING, e));
                            }
                            break;
                        case "NEWLINE_DELIMITED_JSON":
                            inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
                            outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
                            serdeLib = "org.apache.hive.hcatalog.data.JsonSerDe";
                            break;
                        case "PARQUET":
                            inputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
                            outputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
                            serdeLib = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
                            break;
                        case "AVRO":
                            inputFormat = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
                            outputFormat = "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";
                            serdeLib = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
                            break;
                        case "ORC":
                            inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
                            outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
                            serdeLib = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
                            break;
                        default: // Leave format fields null for unsupported formats
                            break;
                    }
                }
                if (externalConfig.getCompression() != null && !"NONE".equals(externalConfig.getCompression())) {
                    tableParameters.put(EXTERNAL_DATA_COMPRESSION, externalConfig.getCompression());
                }
            }
            else if (TableType.isBigQueryManagedIcebergTable(bqTable)) {
                serdeLib = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
                inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
                outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
                storageBuilder.setLocation(bqTable.getBiglakeConfiguration().getStorageUri());
                //tableParameters.put("table_type", "ICEBERG");
            }
            storageBuilder.setStorageFormat(StorageFormat.createNullable(serdeLib, inputFormat, outputFormat));
            storageBuilder.setSerdeParameters(serdeParameters);
        });

        tableBuilder.setParameters(tableParameters.build());

        if (bqTable.getView() != null) {
            tableBuilder.setViewOriginalText(Optional.ofNullable(bqTable.getView().getQuery()));
            tableBuilder.setViewExpandedText(Optional.ofNullable(bqTable.getView().getQuery()));
        }

        return tableBuilder.build();
    }

    static com.google.api.services.bigquery.model.Table hiveTableToBigQueryTable(Table hiveTable, String projectId)
    {
        com.google.api.services.bigquery.model.Table bqTable = new com.google.api.services.bigquery.model.Table();
        bqTable.setTableReference(new TableReference()
                .setProjectId(projectId)
                .setDatasetId(hiveTable.getDatabaseName())
                .setTableId(hiveTable.getTableName()));

        // This call will now throw an exception if any column has an unsupported type
        List<TableFieldSchema> bqFields = hiveTable.getDataColumns().stream()
                .map(BigQueryMetastoreUtils::hiveColumnToBigQueryFieldSchema)
                .collect(Collectors.toList());
        bqTable.setSchema(new TableSchema().setFields(bqFields));

        Optional.ofNullable(hiveTable.getParameters().get("comment"))
                .ifPresent(bqTable::setDescription);

        Map<String, String> labels = hiveTable.getParameters().entrySet().stream()
                .filter(entry -> !entry.getKey().startsWith("bigquery_")
                        && !entry.getKey().equals("comment")
                        && !entry.getKey().equals("EXTERNAL")
                        && !entry.getKey().startsWith("external.")
                        && !entry.getKey().equals(CSV_SKIP_HEADER_LINE_COUNT))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!labels.isEmpty()) {
            bqTable.setLabels(labels);
        }

        if (VIRTUAL_VIEW.name().equals(hiveTable.getTableType())) {
            String viewSql = hiveTable.getViewOriginalText()
                    .orElseThrow(() -> new IllegalArgumentException("View SQL (viewOriginalText) is required for creating a BigQuery view"));
            bqTable.setView(new ViewDefinition().setQuery(viewSql));
        }
        else if (EXTERNAL_TABLE.name().equals(hiveTable.getTableType())) {
            ExternalDataConfiguration externalConfig = new ExternalDataConfiguration();
            Storage storage = hiveTable.getStorage();
            StorageFormat storageFormat = storage.getStorageFormat();

            storage.getOptionalLocation().ifPresent(loc -> externalConfig.setSourceUris(ImmutableList.of(loc)));

            String sourceFormat = hiveTable.getParameters().get(EXTERNAL_DATA_FORMAT);
            if (sourceFormat != null) {
                externalConfig.setSourceFormat(sourceFormat);
                if ("CSV".equals(sourceFormat)) {
                    CsvOptions csvOptions = new CsvOptions();
                    Optional.ofNullable(storage.getSerdeParameters().get(FIELD_DELIM)).ifPresent(csvOptions::setFieldDelimiter);
                    Optional.ofNullable(storage.getSerdeParameters().get(QUOTE_CHAR)).ifPresent(csvOptions::setQuote);
                    Optional.ofNullable(storage.getSerdeParameters().get(SERIALIZATION_ENCODING)).ifPresent(csvOptions::setEncoding);
                    Optional.ofNullable(hiveTable.getParameters().get(CSV_SKIP_HEADER_LINE_COUNT)).map(Long::parseLong).ifPresent(csvOptions::setSkipLeadingRows);
                    externalConfig.setCsvOptions(csvOptions);
                }
            }

            String compression = hiveTable.getParameters().get(EXTERNAL_DATA_COMPRESSION);
            externalConfig.setCompression(compression != null ? compression : "NONE");
            bqTable.setExternalDataConfiguration(externalConfig);
        }
        else { // Assume Managed Table
            Map<String, String> params = hiveTable.getParameters();
            if (params.containsKey(BIGQUERY_TIME_PARTITIONING_FIELD) && params.containsKey(BIGQUERY_TIME_PARTITIONING_TYPE)) {
                bqTable.setTimePartitioning(new TimePartitioning()
                        .setField(params.get(BIGQUERY_TIME_PARTITIONING_FIELD))
                        .setType(params.get(BIGQUERY_TIME_PARTITIONING_TYPE)));
            }
            if (params.containsKey(BIGQUERY_CLUSTERING_FIELDS)) {
                List<String> clusteringFields = ImmutableList.copyOf(params.get(BIGQUERY_CLUSTERING_FIELDS).split(","));
                if (!clusteringFields.isEmpty()) {
                    bqTable.setClustering(new Clustering().setFields(clusteringFields));
                }
            }
        }
        return bqTable;
    }

    // --- Schema/Type Conversion Helpers (Simplified) ---

    static Column bigQueryFieldSchemaToHiveColumn(TableFieldSchema field)
    {
        // This call will now throw for unsupported types or REPEATED mode
        HiveType hiveType = mapBigQueryTypeToHiveType(field.getType(), "REPEATED".equals(field.getMode()), field.getFields());
        return new Column(field.getName(), hiveType, Optional.ofNullable(field.getDescription()), ImmutableMap.of());
    }

    static TableFieldSchema hiveColumnToBigQueryFieldSchema(Column column)
    {
        TableFieldSchema fieldSchema = new TableFieldSchema().setName(column.getName());
        column.getComment().ifPresent(fieldSchema::setDescription);

        String bqType;
        Category category = column.getType().getCategory();
        TypeInfo typeInfo = column.getType().getTypeInfo();

        // Only handle PRIMITIVE category for basic types
        if (category == Category.PRIMITIVE) {
            // This call will throw for unsupported primitive types
            bqType = mapHivePrimitiveTypeToBigQueryType(column.getType());
            fieldSchema.setMode("NULLABLE"); // Default to NULLABLE
        }
        else {
            // Throw for LIST, STRUCT, MAP, UNION etc. in this simplified version
            throw new UnsupportedOperationException("Unsupported Hive type category for BigQuery conversion (only primitives supported): " + category + " for column " + column.getName());
        }
        fieldSchema.setType(bqType);
        return fieldSchema;
    }

    // Simplified: No recursive mapping needed as complex types are unsupported
    // private static String mapHiveTypeToBigQueryTypeFull(HiveType hiveType) {...}

    // Simplified: Only basic BQ types mapped back. No LIST/STRUCT support needed here yet.
    static HiveType mapBigQueryTypeToHiveType(String bqType, boolean isRepeated, List<TableFieldSchema> subFields)
    {
        // Disallow repeated mode (arrays) for now
        if (isRepeated) {
            throw new UnsupportedOperationException("Unsupported BigQuery type: ARRAY (repeated mode) for type " + bqType);
        }
        // Disallow struct/record types for now
        if ("RECORD".equalsIgnoreCase(bqType) || "STRUCT".equalsIgnoreCase(bqType)) {
            throw new UnsupportedOperationException("Unsupported BigQuery type: STRUCT/RECORD");
        }

        if (bqType == null) {
            throw new IllegalArgumentException("BigQuery API type string cannot be null");
        }

        switch (bqType.toUpperCase()) {
            // Supported types
            case "STRING":
                return HiveType.HIVE_STRING;
            case "INTEGER":
            case "INT64":
                return HiveType.HIVE_LONG; // Map all BQ integers to LONG
            case "BOOLEAN":
            case "BOOL":
                return HiveType.HIVE_BOOLEAN;

            // Unsupported types for this basic version
            case "FLOAT":
            case "FLOAT64":
            case "NUMERIC":
            case "BIGNUMERIC":
            case "BYTES":
            case "DATE":
            case "TIMESTAMP":
            case "DATETIME":
            case "TIME":
            case "GEOGRAPHY":
            case "JSON":
                // RECORD/STRUCT already handled above
            default:
                throw new UnsupportedOperationException("Unsupported BigQuery API type: " + bqType);
        }
        // isRepeated logic removed as it's handled by the initial check
    }

    // Simplified: Map only basic Hive primitives to BQ types
    static String mapHivePrimitiveTypeToBigQueryType(HiveType hiveType)
    {
        String typeName = hiveType.getHiveTypeName().toString().toLowerCase();
        // Supported types
        if (typeName.equals("boolean")) {
            return "BOOLEAN";
        }
        if (typeName.equals("tinyint") || typeName.equals("smallint") || typeName.equals("int") || typeName.equals("integer") || typeName.equals("bigint")) {
            return "INT64";
        }
        if (typeName.startsWith("varchar") || typeName.startsWith("char") || typeName.equals("string")) {
            return "STRING";
        }

        // Unsupported types for this basic version
        if (typeName.equals("float") || typeName.equals("double")) {
            throw new UnsupportedOperationException("Unsupported Hive primitive type: " + typeName + " (float/double)");
        }
        if (typeName.startsWith("decimal")) {
            throw new UnsupportedOperationException("Unsupported Hive primitive type: " + typeName + " (decimal)");
        }
        if (typeName.equals("binary")) {
            throw new UnsupportedOperationException("Unsupported Hive primitive type: " + typeName + " (binary)");
        }
        if (typeName.equals("date")) {
            throw new UnsupportedOperationException("Unsupported Hive primitive type: " + typeName + " (date)");
        }
        if (typeName.equals("timestamp")) {
            throw new UnsupportedOperationException("Unsupported Hive primitive type: " + typeName + " (timestamp)");
        }
        if (typeName.equals("timestamptz") || typeName.equals("timestamp with local time zone")) {
            throw new UnsupportedOperationException("Unsupported Hive primitive type: " + typeName + " (timestamp with local time zone)");
        }

        // Catch any other unsupported primitives
        throw new UnsupportedOperationException("Unsupported Hive primitive type for BigQuery API conversion: " + typeName);
    }
}
