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

import static java.util.Objects.requireNonNull;

import com.google.inject.Inject;
import io.opentelemetry.api.trace.Tracer;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.tracing.TracingHiveMetastore;
import io.trino.spi.security.ConnectorIdentity;
import java.util.Optional;

public class BigQueryMetastoreFactory
        implements HiveMetastoreFactory {

    private final HiveMetastore metastore;
    private final boolean impersonationEnabled;

    @Inject
    public BigQueryMetastoreFactory(BigQueryMetastore metastore,
            Tracer tracer) // Inject the BigQueryMetastore implementation
    {
        requireNonNull(metastore, "metastore is null");
        requireNonNull(tracer, "tracer is null");

        // Impersonation based on ConnectorIdentity is NOT handled by this factory.
        // The injected BigQueryMetastore uses the credentials configured globally.
        this.impersonationEnabled = false;

        // Wrap the singleton BigQueryMetastore instance with tracing capabilities.
        this.metastore = new TracingHiveMetastore(tracer, metastore);
    }

    @Override
    public boolean hasBuiltInCaching() {
        // Return true because BigQueryMetastoreModule provides caching capabilities.
        return true;
    }

    @Override
    public boolean isImpersonationEnabled() {
        // Return false signifying that this factory does not create identity-specific metastore instances.
        return impersonationEnabled;
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity) {
        // As impersonation is disabled at this level, always return the shared, traced metastore instance.
        // The optional identity is ignored.
        return metastore;
    }
}
