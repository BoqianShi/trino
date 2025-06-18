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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.cloud.ServiceOptions;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.biglake.BigLakeRestClientFactory;
import io.trino.plugin.iceberg.catalog.biglake.IcebergBigLakeRestCatalogConfig;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.Security;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTSessionCatalog;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TrinoIcebergRestCatalogFactory
        implements TrinoCatalogFactory
{
    private static final String GOOGLE_AUTH_MANAGER = "org.apache.iceberg.gcp.auth.GoogleAuthManager";
    private final TrinoFileSystemFactory fileSystemFactory;
    private final CatalogName catalogName;
    private final String trinoVersion;
    private final URI serverUri;
    private final Optional<String> warehouse;
    private final SessionType sessionType;
    private final Security securityType;
    private final SecurityProperties securityProperties;
    private final String bigLakeProjectId;
    private final String authType;
    private final boolean restMetricReportingEnabled = false;
    private boolean uniqueTableLocation;
    @GuardedBy("this")
    private RESTSessionCatalog icebergCatalog;

    @Inject
    public TrinoIcebergRestCatalogFactory(
            TrinoFileSystemFactory fileSystemFactory,
            CatalogName catalogName,
            IcebergRestCatalogConfig restConfig,
            IcebergBigLakeRestCatalogConfig bigLakeConfig,
            SecurityProperties securityProperties,
            IcebergConfig icebergConfig,
            NodeVersion nodeVersion)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        requireNonNull(restConfig, "restConfig is null");
        this.serverUri = restConfig.getBaseUri();
        this.warehouse = restConfig.getWarehouse();
        this.sessionType = restConfig.getSessionType();
        this.securityType = restConfig.getSecurity();
        this.securityProperties = requireNonNull(securityProperties, "securityProperties is null");
        requireNonNull(icebergConfig, "icebergConfig is null");
        this.uniqueTableLocation = icebergConfig.isUniqueTableLocation();
        this.authType = bigLakeConfig.getAuthType();
        this.bigLakeProjectId = Optional.ofNullable(bigLakeConfig.getProjectId()).orElse(ServiceOptions.getDefaultProjectId());
    }

    static RESTClient defaultHTTPClient(Map<String, String> config)
    {
        return HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build();
    }

    @Override
    public synchronized TrinoCatalog create(ConnectorIdentity identity)
    {
        // Creation of the RESTSessionCatalog is lazy due to required network calls
        // for authorization and config route
        if (icebergCatalog == null) {
            ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
            properties.put(CatalogProperties.URI, serverUri.toString());
            properties.put("trino-version", trinoVersion);
            properties.putAll(securityProperties.get());
            if (Objects.equals(authType, GOOGLE_AUTH_MANAGER)) {
                checkArgument(warehouse.isPresent(), "Warehouse location (iceberg.rest-catalog.warehouse) must be set when using BigLake REST API.");
                properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse.get());
                properties.put("biglake-projectId", bigLakeProjectId);
                properties.put("rest-metrics-reporting-enabled", String.valueOf(restMetricReportingEnabled));
                this.uniqueTableLocation = false;
            }
            else {
                warehouse.ifPresent(location -> properties.put(CatalogProperties.WAREHOUSE_LOCATION, location));
            }
            ImmutableMap<String, String> propertiesMap = properties.buildOrThrow();
            RESTSessionCatalog icebergCatalogInstance = new RESTSessionCatalog(
                    restClientFactory(propertiesMap).orElse(TrinoIcebergRestCatalogFactory::defaultHTTPClient),
                    (context, config) -> {
                        ConnectorIdentity currentIdentity = (context.wrappedIdentity() != null)
                                ? ((ConnectorIdentity) context.wrappedIdentity())
                                : ConnectorIdentity.ofUser("fake");
                        return new ForwardingFileIo(fileSystemFactory.create(currentIdentity));
                    });
            icebergCatalogInstance.initialize(catalogName.toString(), propertiesMap);

            icebergCatalog = icebergCatalogInstance;
        }

        return new TrinoRestCatalog(icebergCatalog, catalogName, sessionType, trinoVersion, uniqueTableLocation);
    }

    Optional<Function<Map<String, String>, RESTClient>> restClientFactory(Map<String, String> config)
    {
        if (Objects.equals(authType, GOOGLE_AUTH_MANAGER)) {
            BigLakeRestClientFactory bigLakeRESTClientFactory = new BigLakeRestClientFactory();
            return Optional.of(bigLakeRESTClientFactory);
        }
        return Optional.empty();
    }
}
