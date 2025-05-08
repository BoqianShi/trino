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

import com.google.api.services.bigquery.model.Table;

public class TableType
{
    private TableType()
    {
        // Private constructor to prevent instantiation
    }

    public static boolean isBigQueryOSSTable(Table bigQueryTable)
    {
        return bigQueryTable.getSchema().getForeignTypeInfo() != null
                && bigQueryTable.getSchema().getForeignTypeInfo().getTypeSystem().equalsIgnoreCase("HIVE");
    }

    public static boolean isBigQueryExternalTable(Table bigQueryTable)
    {
        return bigQueryTable.getExternalDataConfiguration() != null;
    }

    public static boolean isBigQueryManagedIcebergTable(Table bigQueryTable)
    {
        return bigQueryTable.getBiglakeConfiguration() != null && bigQueryTable.getBiglakeConfiguration().getTableFormat().equalsIgnoreCase("ICEBERG");
    }

    public static boolean isBigQueryNativeTable(Table bigQueryTable)
    {
        return bigQueryTable.getType().equalsIgnoreCase("TABLE");
    }
}
