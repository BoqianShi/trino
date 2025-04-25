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
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hive.AllowHiveTableRename;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;

import java.util.concurrent.Executor;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public final class BigQueryMetastoreModule
        extends AbstractConfigurationAwareModule
{
    /**
     * Helper method to create bounded executors.
     */
    private static Executor createExecutor(String nameTemplate, int threads)
    {
        if (threads <= 1) { // Use direct executor for 1 thread
            return directExecutor();
        }
        return new BoundedExecutor(newCachedThreadPool(daemonThreadsNamed(nameTemplate)), threads);
    }

    @Override
    protected void setup(Binder binder)
    {
        // Bind the configuration
        configBinder(binder).bindConfig(BigQueryMetastoreConfig.class);

        // Bind the core BigQuery Metastore implementation and its factory
        binder.bind(BigQueryMetastore.class).in(Scopes.SINGLETON); // Needs definition
        newOptionalBinder(binder,
                Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class)).setDefault()
                .to(BigQueryMetastoreFactory.class) // Needs definition (adapted from older Glue Factory)
                .in(Scopes.SINGLETON);
        // Bind the concrete factory class as well for potential direct injection elsewhere
        binder.bind(BigQueryMetastoreFactory.class).in(Scopes.SINGLETON);

        // Export the Factory for JMX monitoring under the Metastore's name
        newExporter(binder).export(BigQueryMetastoreFactory.class)
                .as(generator -> generator.generatedNameOf(BigQueryMetastore.class));

        // Configure table renaming behavior
        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(false);
    }

    /**
     * Provides a general-purpose executor for BigQuery metastore operations.
     */
    @Provides
    @Singleton
    //@ForBigQueryMetastore // Use annotation to qualify if other executors are added later
    public Executor createExecutor(BigQueryMetastoreConfig config) // Use BigQueryMetastoreConfig
    {
        // Assumes BigQueryMetastoreConfig has a getThreads() method
        return createExecutor("hive-bigquery-ops-%s", config.getThreads());
    }
}
