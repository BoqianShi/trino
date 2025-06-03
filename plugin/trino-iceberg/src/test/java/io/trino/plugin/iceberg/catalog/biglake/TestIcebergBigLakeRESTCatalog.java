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
package io.trino.plugin.iceberg.catalog.biglake;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Integration tests for Iceberg BigLake REST catalog.
 * This test requires GCP authentication to be configured on the machine where the test is run.
 * It uses Application Default Credentials (ADC) by default.
 * The following environment variables must be set:
 * - GCP_PROJECT_ID: The GCP project ID to use.
 * - BIGLAKE_REST_URI: The REST URI for the BigLake catalog (e.g., https://...)
 * - TEMPORARY_GCS_BUCKET: The GCS URI you want to use.
 */
@SuppressWarnings("ConstantValue")
@TestInstance(PER_CLASS)
public class TestIcebergBigLakeRESTCatalog
        extends AbstractTestQueryFramework
{
    static final String BIGLAKE_REST_URI_ENV_VARIABLE = "BIGLAKE_REST_URI";
    static final String TEMPORARY_GCS_BUCKET_ENV_VARIABLE = "TEMPORARY_GCS_BUCKET";
    private static final String CATALOG_NAME = "iceberg";

    private static final int NATIVE_GCS_MIN_VERSION_THRESHOLD = 439; // Trino versions > this use native GCS

    private final String gcpProjectId;
    private final String biglakeRestUri;
    private final String gcsWarehouseUri;
    private final int trinoVersionNumber = 432; // Set the version here

    public TestIcebergBigLakeRESTCatalog()
    {
        this.gcpProjectId = requireNonNull(
                System.getenv("GOOGLE_CLOUD_PROJECT"),
                "Please set the GOOGLE_CLOUD_PROJECT env variable");

        this.biglakeRestUri = requireNonNull(
                System.getenv(BIGLAKE_REST_URI_ENV_VARIABLE),
                () -> String.format("Please set the %s env variable", BIGLAKE_REST_URI_ENV_VARIABLE));

        this.gcsWarehouseUri = requireNonNull(
                System.getenv(TEMPORARY_GCS_BUCKET_ENV_VARIABLE),
                () -> String.format("Please set the %s env variable", TEMPORARY_GCS_BUCKET_ENV_VARIABLE));
    }

    private Map<String, String> getCatalogProperties()
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("iceberg.catalog.type", "rest");
        properties.put("iceberg.rest-catalog.security", "BIGLAKE");
        properties.put("iceberg.rest-catalog.biglake.project-id", gcpProjectId);
        properties.put("iceberg.rest-catalog.uri", biglakeRestUri);
        properties.put("iceberg.rest-catalog.warehouse", gcsWarehouseUri);

        if (trinoVersionNumber > NATIVE_GCS_MIN_VERSION_THRESHOLD) {
            properties.put("fs.native-gcs.enabled", "true");
        }
        else {
            properties.put("fs.hadoop.enabled", "true");
            properties.put("hive.gcs.use-access-token", "false");
        }
        return ImmutableMap.copyOf(properties);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setCatalogSessionProperty(CATALOG_NAME, "statistics_enabled", "false")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new io.trino.plugin.iceberg.IcebergPlugin());

        Map<String, String> catalogProperties = getCatalogProperties();
        queryRunner.createCatalog(CATALOG_NAME, "iceberg", catalogProperties);

        return queryRunner;
    }

    /**
     * Verifies basic schema listing functionality, corresponding to the REST API: listNamespaces.
     * Trino SQL: SHOW SCHEMAS.
     * Creates a schema and checks if it appears in the list of schemas, then drops it.
     */
    @Test
    public void testShowSchemas()
    {
        String schemaName = "test_show_schemas_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        try {
            assertQuery(
                    format("SHOW SCHEMAS LIKE '%s'", schemaName),
                    format("VALUES '%s'", schemaName));
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName);
        }
    }

    /**
     * Verifies schema creation functionality, corresponding to the REST API: createNamespace.
     * Trino SQL: CREATE SCHEMA.
     * Creates a schema and verifies its existence using SHOW SCHEMAS.
     */
    @Test
    public void testCreateSchema()
    {
        String schemaName = "test_create_schema_" + randomNameSuffix();
        try {
            assertUpdate("CREATE SCHEMA " + schemaName);
            assertQuery("SHOW SCHEMAS LIKE '" + schemaName + "'", "VALUES ('" + schemaName + "')");
        }
        finally {
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    /**
     * Verifies schema dropping functionality, corresponding to the REST API: dropNamespace.
     * Trino SQL: DROP SCHEMA.
     * Creates a schema, verifies its existence, drops it, and then verifies its removal.
     */
    @Test
    public void testDropSchema()
    {
        String schemaName = "test_drop_schema_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        assertQuery("SHOW SCHEMAS LIKE '" + schemaName + "'", "VALUES ('" + schemaName + "')");

        assertUpdate("DROP SCHEMA " + schemaName);
        assertThat(computeActual("SHOW SCHEMAS LIKE '" + schemaName + "'").getRowCount()).isEqualTo(0);
    }

    /**
     * Verifies basic table creation functionality, corresponding to the REST API: createTable.
     * Trino SQL: CREATE TABLE.
     * Creates a schema and a table within it, then verifies the table's existence using SHOW TABLES.
     */
    @Test
    public void testCreateTable()
    {
        String schemaName = "test_create_table_schema_" + randomNameSuffix();
        String tableName = "test_create_table_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        try {
            assertUpdate(format("CREATE TABLE %s.%s (id INT, data VARCHAR)", schemaName, tableName));

            assertQuery(
                    format("SHOW TABLES FROM %s LIKE '%s'", schemaName, tableName),
                    format("VALUES('%s')", tableName));
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        }
    }

    /**
     * Verifies table listing functionality within a schema, corresponding to the REST API: listTables.
     * Trino SQL: SHOW TABLES FROM schema.
     * Creates a schema with multiple tables and checks if they are correctly listed,
     * including tests with LIKE clauses.
     */
    @Test
    public void testShowTables()
    {
        String schemaName = "test_show_tables_schema_" + randomNameSuffix();
        String tableName1 = "table_apple_" + randomNameSuffix(); // Lexicographically smaller
        String tableName2 = "table_zebra_" + randomNameSuffix(); // Lexicographically larger

        assertUpdate("CREATE SCHEMA " + schemaName);
        try {
            assertUpdate(format("CREATE TABLE %s.%s (id INT)", schemaName, tableName1));
            assertUpdate(format("CREATE TABLE %s.%s (name VARCHAR)", schemaName, tableName2));

            assertQuery(
                    format("SHOW TABLES FROM %s", schemaName),
                    format("VALUES ('%s'), ('%s')", tableName1, tableName2)); // Assuming alphabetical order

            assertQuery(
                    format("SHOW TABLES FROM %s LIKE 'table_apple%%'", schemaName),
                    format("VALUES ('%s')", tableName1));

            assertQuery(
                    format("SHOW TABLES FROM %s LIKE '%%zebra%%'", schemaName),
                    format("VALUES ('%s')", tableName2));

            assertQuery(
                    format("SHOW TABLES FROM %s LIKE 'table_non_existent%%'", schemaName),
                    "SELECT 1 WHERE false"); // No rows expected
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        }
    }

    /**
     * Verifies table dropping functionality, corresponding to the REST API: dropTable.
     * Trino SQL: DROP TABLE.
     * Creates a table, verifies its existence, drops it, and then verifies its removal.
     */
    @Test
    public void testDropTable()
    {
        String schemaName = "test_drop_table_schema_" + randomNameSuffix();
        String tableName = "test_drop_table_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        try {
            assertUpdate(format("CREATE TABLE %s.%s (id INT, data VARCHAR)", schemaName, tableName));
            assertQuery(format("SHOW TABLES FROM %s LIKE '%s'", schemaName, tableName), format("VALUES('%s')", tableName));

            assertUpdate(format("DROP TABLE %s.%s", schemaName, tableName));
            assertThat(computeActual(format("SHOW TABLES FROM %s LIKE '%s'", schemaName, tableName)).getRowCount()).isEqualTo(0);
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        }
    }

    /**
     * Verifies basic data insertion into an Iceberg table.
     * This indirectly tests table metadata updates (new snapshot) via the REST API's updateTable endpoint
     * as a result of a commit operation.
     * Trino SQL: INSERT INTO.
     */
    @Test
    public void testInsert()
    {
        String schemaName = "test_insert_schema_" + randomNameSuffix();
        String tableName = "test_insert_table_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        try {
            assertUpdate(format("CREATE TABLE %s.%s (id INT, name VARCHAR)", schemaName, tableName));
            assertUpdate(format("INSERT INTO %s.%s VALUES (1, 'Trino'), (2, 'BigLake')", schemaName, tableName), 2);
            assertQuery("SELECT count(*) FROM " + schemaName + "." + tableName, "SELECT 2");
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        }
    }

    /**
     * Verifies basic data selection from an Iceberg table.
     * This indirectly tests table metadata loading via the REST API's loadTable (getTable) endpoint.
     * Trino SQL: SELECT FROM.
     */
    @Test
    public void testSelect()
    {
        String schemaName = "test_select_schema_" + randomNameSuffix();
        String tableName = "test_select_table_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        try {
            assertUpdate(format("CREATE TABLE %s.%s (id INT, name VARCHAR, region VARCHAR)", schemaName, tableName));
            assertUpdate(format("INSERT INTO %s.%s VALUES (1, 'Trino', 'us-west1'), (2, 'BigLake', 'us-central1'), (3, 'Iceberg', 'us-central1')", schemaName, tableName), 3);

            assertQuery(
                    "SELECT name, region FROM " + schemaName + "." + tableName + " WHERE region = 'us-central1' ORDER BY name",
                    "VALUES ('BigLake', 'us-central1'), ('Iceberg', 'us-central1')");
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        }
    }

    /**
     * Verifies table schema description and basic querying, corresponding to REST API: loadTable (getTable).
     * Trino SQL: DESCRIBE table, SELECT FROM table.
     * Creates a table with various data types, describes its schema, and queries its content.
     */
    @Test
    public void testDescribeTableAndQuery()
    {
        String schemaName = "describe_schema_" + randomNameSuffix();
        String tableName = "describe_table_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        try {
            assertUpdate(format("CREATE TABLE %s.%s (id INT, name VARCHAR(50), price DECIMAL(10,2), event_date DATE)", schemaName, tableName));
            assertUpdate(format("INSERT INTO %s.%s VALUES (1, 'Product A', 19.99, DATE '2023-01-15'), (2, 'Product B', 100.00, DATE '2023-02-20')", schemaName, tableName), 2);

            MaterializedResult result = computeActual(format("DESCRIBE %s.%s", schemaName, tableName));
            assertThat(result.getMaterializedRows().stream().map(row -> row.getField(0) + " " + row.getField(1))) // Column Name + Type
                    .containsExactlyInAnyOrder(
                            "id integer",
                            "name varchar",
                            "price decimal(10,2)",
                            "event_date date");

            assertQuery(format("SELECT name, price FROM %s.%s WHERE id = 1", schemaName, tableName), "VALUES ('Product A', 19.99)");
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        }
    }

    /**
     * Verifies creation and querying of tables with Iceberg complex types (ARRAY, MAP, ROW).
     * This tests the catalog's ability to handle complex schema definitions and Trino's ability
     * to interact with these types.
     * Trino SQL: CREATE TABLE with complex types, INSERT INTO, SELECT.
     */
    @Test
    public void testComplexTypes()
    {
        String schemaName = "complex_types_schema_" + randomNameSuffix();
        String tableName = "complex_types_table_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        try {
            assertUpdate(format(
                    "CREATE TABLE %s.%s (" +
                            "id INT, " +
                            "my_array ARRAY(VARCHAR), " +
                            "my_map MAP(VARCHAR, INT), " +
                            "my_struct ROW(field_a VARCHAR, field_b BIGINT, nested_array ARRAY(INT))" +
                            ")", schemaName, tableName));

            // Insert data with complex types
            assertUpdate(format(
                    "INSERT INTO %s.%s VALUES " +
                            "(1, ARRAY['apple', 'banana'], MAP(ARRAY['k1', 'k2'], ARRAY[10, 20]), ROW('hello', 100, ARRAY[1,2,3])), " +
                            "(2, ARRAY['orange'], MAP(ARRAY['k3'], ARRAY[30]), ROW('world', 200, ARRAY[4,5]))",
                    schemaName, tableName), 2);

            assertQuery(format("SELECT my_array[1] FROM %s.%s WHERE id = 1", schemaName, tableName), "VALUES ('apple')");

            assertQuery(format("SELECT my_map['k2'] FROM %s.%s WHERE id = 1", schemaName, tableName), "VALUES (20)");

            assertQuery(format("SELECT my_struct.field_a FROM %s.%s WHERE id = 2", schemaName, tableName), "VALUES ('world')");
            assertQuery(format("SELECT my_struct.nested_array[2] FROM %s.%s WHERE id = 1", schemaName, tableName), "VALUES (2)");

            assertQuery(format("SELECT my_array FROM %s.%s WHERE id = 2", schemaName, tableName), "VALUES (ARRAY['orange'])");
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        }
    }

    /**
     * Tests behaviors related to schema existence, indirectly covering REST API: namespaceExists.
     * Trino SQL: CREATE SCHEMA, CREATE SCHEMA IF NOT EXISTS, DROP SCHEMA, SHOW SCHEMAS.
     */
    @Test
    public void testSchemaExistsBehavior()
    {
        String schemaName = "schema_exists_test_" + randomNameSuffix();

        assertThat(computeActual(format("SHOW SCHEMAS LIKE '%s'", schemaName)).getRowCount())
                .as("Schema should not exist initially")
                .isEqualTo(0);

        assertUpdate(format("CREATE SCHEMA %s", schemaName));
        assertThat(computeActual(format("SHOW SCHEMAS LIKE '%s'", schemaName)).getRowCount())
                .as("Schema should exist after creation")
                .isEqualTo(1);

        assertThatThrownBy(() -> getQueryRunner().execute(format("CREATE SCHEMA %s", schemaName)))
                .isInstanceOf(io.trino.testing.QueryFailedException.class)
                .hasMessageContaining(format("Schema '%s.%s' already exists", CATALOG_NAME, schemaName));

        assertQuerySucceeds(format("CREATE SCHEMA IF NOT EXISTS %s", schemaName));
        assertThat(computeActual(format("SHOW SCHEMAS LIKE '%s'", schemaName)).getRowCount())
                .as("Schema should still exist")
                .isEqualTo(1);

        assertUpdate(format("DROP SCHEMA %s", schemaName));
        assertThat(computeActual(format("SHOW SCHEMAS LIKE '%s'", schemaName)).getRowCount())
                .as("Schema should not exist after drop")
                .isEqualTo(0);

        assertThatThrownBy(() -> getQueryRunner().execute(format("DROP SCHEMA %s", schemaName)))
                .isInstanceOf(io.trino.testing.QueryFailedException.class)
                .hasMessageContaining(format("Schema '%s.%s' does not exist", CATALOG_NAME, schemaName));

        assertUpdate(format("CREATE SCHEMA IF NOT EXISTS %s", schemaName));
        assertThat(computeActual(format("SHOW SCHEMAS LIKE '%s'", schemaName)).getRowCount())
                .as("Schema should exist after CREATE IF NOT EXISTS")
                .isEqualTo(1);

        assertUpdate(format("DROP SCHEMA %s", schemaName));
    }

    /**
     * Tests behaviors related to table existence, indirectly covering REST API: tableExists.
     * Trino SQL: CREATE TABLE, CREATE TABLE IF NOT EXISTS, DROP TABLE, DROP TABLE IF EXISTS, SHOW TABLES.
     */
    @Test
    public void testTableExistsBehavior()
    {
        String schemaName = "table_exists_schema_" + randomNameSuffix();
        String tableName = "table_exists_test_" + randomNameSuffix(); // Original table name
        String qualifiedTableName = format("%s.%s", schemaName, tableName);
        String catalogQualifiedTableName = format("%s.%s", CATALOG_NAME, qualifiedTableName);

        String anotherTableName = "another_table_exists_test_" + randomNameSuffix();
        String qualifiedAnotherTableName = format("%s.%s", schemaName, anotherTableName);

        assertUpdate(format("CREATE SCHEMA %s", schemaName));
        try {
            // 1. Table does not exist initially in the schema
            assertThat(computeActual(format("SHOW TABLES FROM %s LIKE '%s'", schemaName, tableName)).getRowCount())
                    .as("Table should not exist initially in the schema")
                    .isEqualTo(0);

            // 2. Create table
            assertUpdate(format("CREATE TABLE %s (id INT)", qualifiedTableName));
            assertThat(computeActual(format("SHOW TABLES FROM %s LIKE '%s'", schemaName, tableName)).getRowCount())
                    .as("Table should exist after creation")
                    .isEqualTo(1);

            // 3. Attempt to create existing table (should fail)
            assertThatThrownBy(() -> getQueryRunner().execute(format("CREATE TABLE %s (name VARCHAR)", qualifiedTableName)))
                    .isInstanceOf(io.trino.testing.QueryFailedException.class)
                    .hasMessageContaining(format("Table '%s' already exists", catalogQualifiedTableName));

            // 4. Use CREATE TABLE IF NOT EXISTS on an existing table (should be a no-op)
            assertQuerySucceeds(format("CREATE TABLE IF NOT EXISTS %s (id INT, another_col VARCHAR)", qualifiedTableName));
            assertThat(computeActual(format("SHOW TABLES FROM %s LIKE '%s'", schemaName, tableName)).getRowCount())
                    .as("Table should still exist")
                    .isEqualTo(1);
            MaterializedResult describeResult = computeActual(format("DESCRIBE %s", qualifiedTableName));
            assertThat(describeResult.getMaterializedRows().stream().map(row -> row.getField(0)))
                    .as("Table schema should be the original one (only 'id')")
                    .containsExactly("id");

            // 5. Drop table
            assertUpdate(format("DROP TABLE %s", qualifiedTableName));
            assertThat(computeActual(format("SHOW TABLES FROM %s LIKE '%s'", schemaName, tableName)).getRowCount())
                    .as("Table should not exist after drop")
                    .isEqualTo(0);

            // 6. Attempt to drop non-existent table (should fail)
            String expectedDropNonExistentTableError = format("line 1:1: Table '%s' does not exist", catalogQualifiedTableName);
            assertThatThrownBy(() -> getQueryRunner().execute(format("DROP TABLE %s", qualifiedTableName)))
                    .isInstanceOf(io.trino.testing.QueryFailedException.class)
                    .hasMessage(expectedDropNonExistentTableError);

            // 7. Use DROP TABLE IF EXISTS on a non-existent table (should be a no-op)
            assertQuerySucceeds(format("DROP TABLE IF EXISTS %s", qualifiedTableName));

            // 8. Use CREATE TABLE IF NOT EXISTS with a NEW, DIFFERENT table name (should create it)
            assertUpdate(format("CREATE TABLE IF NOT EXISTS %s (data VARCHAR)", qualifiedAnotherTableName)); // Using qualifiedAnotherTableName
            assertThat(computeActual(format("SHOW TABLES FROM %s LIKE '%s'", schemaName, anotherTableName)).getRowCount()) // Check for anotherTableName
                    .as("The new table should exist after CREATE IF NOT EXISTS")
                    .isEqualTo(1);
            describeResult = computeActual(format("DESCRIBE %s", qualifiedAnotherTableName)); // Describe anotherTableName
            assertThat(describeResult.getMaterializedRows().stream().map(row -> row.getField(0)))
                    .as("The new table schema should be ('data')")
                    .containsExactly("data");

            // 9. Use DROP TABLE IF EXISTS on an existing table (the new one we just created)
            assertUpdate(format("DROP TABLE IF EXISTS %s", qualifiedAnotherTableName)); // Drop anotherTableName
            assertThat(computeActual(format("SHOW TABLES FROM %s LIKE '%s'", schemaName, anotherTableName)).getRowCount()) // Check anotherTableName
                    .as("The new table should not exist after DROP IF EXISTS")
                    .isEqualTo(0);
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        }
    }

    /**
     * Covers REST API: updateTable (POST .../tables/{table}) - for table properties.
     * Trino SQL: ALTER TABLE ... SET PROPERTIES ...
     * This test verifies that updating the 'format' table property via ALTER TABLE
     * succeeds with the REST catalog.
     */
    @Test
    public void testUpdateTableFormatProperty()
    {
        String schemaName = "update_tbl_format_schema_" + randomNameSuffix();
        String tableName = "update_tbl_format_table_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        try {
            assertUpdate(format("CREATE TABLE %s.%s (id INT)", schemaName, tableName));

            String createTableOutputBeforeAlter = (String) computeScalar(format("SHOW CREATE TABLE %s.%s", schemaName, tableName));
            assertThat(createTableOutputBeforeAlter)
                    .as("Initial format should be PARQUET (or the catalog default)")
                    .containsIgnoringCase("format = 'PARQUET'"); // Use IgnoringCase if default might vary slightly
            assertThat(createTableOutputBeforeAlter)
                    .as("Initial format_version should be 2 (catalog default)")
                    .contains("format_version = 2");

            String alterTableSql = format("ALTER TABLE %s.%s SET PROPERTIES format = 'ORC'",
                    schemaName, tableName);
            assertUpdate(alterTableSql); // Expect this to succeed based on your manual test

            String createTableOutputAfterSuccessfulAlter = (String) computeScalar(format("SHOW CREATE TABLE %s.%s", schemaName, tableName));
            assertThat(createTableOutputAfterSuccessfulAlter)
                    .as("format should be 'ORC' after successful ALTER")
                    .containsIgnoringCase("format = 'ORC'");
            assertThat(createTableOutputAfterSuccessfulAlter)
                    .as("format should NOT be 'PARQUET' after successful ALTER to ORC")
                    .doesNotContainIgnoringCase("format = 'PARQUET'");

            assertThat(createTableOutputAfterSuccessfulAlter)
                    .as("format_version should remain 2 (catalog default) unless explicitly changed")
                    .contains("format_version = 2");

            assertUpdate(format("INSERT INTO %s.%s VALUES (1), (2)", schemaName, tableName), 2);
            assertQuery(format("SELECT count(*) FROM %s.%s", schemaName, tableName), "SELECT 2");
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        }
    }

    /**
     * Covers REST API: renameTable (POST /v1/{prefix}/tables/rename)
     * Trino SQL: ALTER TABLE ... RENAME TO ...
     * This test majorly serves as a placeholder for future use. Currently only verifies that renaming tables is not supported and results in an error.
     */

    /**
     * Covers REST API: updateTable (POST .../tables/{table}) - for schema evolution
     * Trino SQL: ALTER TABLE ... ADD COLUMN ...
     */
    @Test
    public void testUpdateTableAddColumn()
    {
        String schemaName = "alter_add_col_schema_" + randomNameSuffix();
        String tableName = "alter_add_col_table_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA " + schemaName);
        try {
            assertUpdate(format("CREATE TABLE %s.%s (id INT, original_data VARCHAR)", schemaName, tableName));
            assertUpdate(format("INSERT INTO %s.%s (id, original_data) VALUES (1, 'data1'), (2, 'data2')", schemaName, tableName), 2);

            assertUpdate(format("ALTER TABLE %s.%s ADD COLUMN new_col_int INT", schemaName, tableName));

            MaterializedResult describeResult = computeActual(format("DESCRIBE %s.%s", schemaName, tableName));
            assertThat(describeResult.getMaterializedRows().stream().map(row -> row.getField(0))) // Column names
                    .contains("id", "original_data", "new_col_int");
            assertThat(describeResult.getMaterializedRows().stream()
                    .filter(row -> "new_col_int".equals(row.getField(0)))
                    .anyMatch(row -> "integer".equals(row.getField(1)))) // Type check for new column
                    .isTrue();

            assertUpdate(format("INSERT INTO %s.%s (id, original_data, new_col_int) VALUES (3, 'data3', 100)", schemaName, tableName), 1);

            assertQuery(
                    format("SELECT id, original_data, new_col_int FROM %s.%s ORDER BY id", schemaName, tableName),
                    "VALUES (1, 'data1', NULL), (2, 'data2', NULL), (3, 'data3', 100)");

            assertUpdate(format("ALTER TABLE %s.%s ADD COLUMN another_col_varchar VARCHAR COMMENT 'This is a test comment'", schemaName, tableName));
            describeResult = computeActual(format("DESCRIBE %s.%s", schemaName, tableName));
            assertThat(describeResult.getMaterializedRows().stream().map(row -> row.getField(0)))
                    .contains("another_col_varchar");
            assertThat(describeResult.getMaterializedRows().stream()
                    .filter(row -> "another_col_varchar".equals(row.getField(0)))
                    .anyMatch(row -> "This is a test comment".equals(row.getField(3))))
                    .isTrue();
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        }
    }

    /**
     * Currently, views are not supported by the BigLake REST catalog implementation being tested.
     * This test verifies that attempting to create a view results in the expected "not supported" error.
     * Test functions for views are majorly kept as a placeholder for future needs.
     */
    @Test
    public void testCreateViewFailsAsUnsupported()
    {
        String schemaName = "view_schema_" + randomNameSuffix();
        String tableName = "view_source_table_" + randomNameSuffix();
        String viewName = "test_view_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA " + schemaName);
        try {
            assertUpdate(format("CREATE TABLE %s.%s (id INT, data VARCHAR)", schemaName, tableName));
            assertUpdate(format("INSERT INTO %s.%s VALUES (1, 'apple'), (2, 'banana')", schemaName, tableName), 2);

            // Attempt to Create View and expect failure
            String createViewSql = format("CREATE VIEW %s.%s AS SELECT id, data FROM %s.%s WHERE id = 1", schemaName, viewName, schemaName, tableName);

            assertThatThrownBy(() -> getQueryRunner().execute(createViewSql))
                    .isInstanceOf(io.trino.testing.QueryFailedException.class)
                    .hasMessageContaining("createView is not supported for Iceberg REST catalog");

            assertThat(computeActual(format("SHOW TABLES FROM %s LIKE '%s'", schemaName, viewName)).getRowCount())
                    .as("View should not exist after failed creation attempt")
                    .isEqualTo(0);
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        }
    }

    /**
     * Currently, views are not supported by the BigLake REST catalog implementation being tested.
     * This test verifies that attempting to show create a view results in the expected "not supported" error.
     * Test functions for views are majorly kept as a placeholder for future needs.
     */
    @Test
    public void testShowCreateViewAndQueryAsUnsupported()
    {
        String schemaName = "show_view_schema_" + randomNameSuffix();
        String tableName = "show_view_source_table_" + randomNameSuffix();
        String viewName = "show_test_view_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA " + schemaName);
        try {
            assertUpdate(format("CREATE TABLE %s.%s (item_id INT, item_name VARCHAR, category VARCHAR)", schemaName, tableName));
            assertUpdate(format("INSERT INTO %s.%s VALUES (101, 'Laptop', 'Electronics'), (102, 'Book', 'Literature')", schemaName, tableName), 2);

            String viewQuery = format("SELECT item_id, item_name FROM %s.%s WHERE category = 'Electronics'", schemaName, tableName);
            String createViewSql = format("CREATE VIEW %s.%s AS %s", schemaName, viewName, viewQuery);

            assertThatThrownBy(() -> getQueryRunner().execute(createViewSql))
                    .isInstanceOf(io.trino.testing.QueryFailedException.class)
                    .hasMessageContaining("createView is not supported for Iceberg REST catalog");

            String showCreateViewSql = format("SHOW CREATE VIEW %s.%s", schemaName, viewName);
            String expectedViewFullName = format("%s.%s.%s", CATALOG_NAME, schemaName, viewName);
            String expectedErrorMessageRegex = format("line 1:1: View '%s' does not exist", expectedViewFullName);

            assertThatThrownBy(() -> getQueryRunner().execute(showCreateViewSql))
                    .isInstanceOf(io.trino.testing.QueryFailedException.class)
                    .hasMessage(expectedErrorMessageRegex);

            assertThat(computeActual(format("SHOW TABLES FROM %s LIKE '%s'", schemaName, viewName)).getRowCount())
                    .as("View should not exist after failed creation attempt")
                    .isEqualTo(0);
        }
        finally {
            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        }
    }
}
