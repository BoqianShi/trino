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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hive.AllowHiveTableRename;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public final class BigQueryMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        BigQueryMetastoreConfig bigQueryMetastoreConfig = buildConfigObject(BigQueryMetastoreConfig.class);

        binder.bind(BigQueryMetastore.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder,
                Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class)).setDefault()
                .to(BigQueryMetastoreFactory.class)
                .in(Scopes.SINGLETON);

        binder.bind(BigQueryMetastoreFactory.class).in(Scopes.SINGLETON);

        newExporter(binder).export(BigQueryMetastoreFactory.class)
                .as(generator -> generator.generatedNameOf(BigQueryMetastore.class));

        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(false);
    }
}
