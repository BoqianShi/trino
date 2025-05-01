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

import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.inject.Inject;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BigQueryHiveMetastoreFactory
        implements HiveMetastoreFactory
{
    private final BigQueryHiveMetastore metastore; // Our adapter instance

    @Inject
    public BigQueryHiveMetastoreFactory(BigQueryHiveMetastore metastore) // Guice injects the singleton
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    // Optional: If you want to expose the metastore for JMX via the factory, like in your example
    @Managed
    @Flatten
    public BigQueryHiveMetastore getJmxManagedMetastore()
    {
        return metastore;
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        // Assuming your BigQueryMetastoreClient does not support standard Hive impersonation
        return false;
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        // Always return the same singleton instance.
        // The ConnectorIdentity is ignored as impersonation is not enabled.
        return metastore;
    }
}
