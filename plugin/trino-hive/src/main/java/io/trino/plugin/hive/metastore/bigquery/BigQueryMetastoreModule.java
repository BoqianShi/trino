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

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.AllowHiveTableRename;
import java.io.IOException;
import java.security.GeneralSecurityException;


public final class BigQueryMetastoreModule
        extends AbstractConfigurationAwareModule {

    /***
     @Provides
     @Singleton public static void createBigQueryMetastoreCache( // Needs Interface + Impls
     CachingHiveMetastoreConfig config,
     CatalogName catalogName,
     NodeManager nodeManager)
     {
     Duration metadataCacheTtl = config.getMetastoreCacheTtl();
     Duration statsCacheTtl = config.getStatsCacheTtl();

     boolean enabled = nodeManager.getCurrentNode().isCoordinator() &&
     (metadataCacheTtl.toMillis() > 0 || statsCacheTtl.toMillis() > 0);

     if (enabled) {
     // Replace with actual implementation, possibly adapting InMemoryGlueCache
     }
     }***/

    @Provides
    @Singleton
    // Provides the actual Google BigQuery client, configured via BigQueryMetastoreConfig
    public static Bigquery createBigQueryClient(BigQueryMetastoreConfig config) {
        try {
            HttpCredentialsAdapter httpCredentialsAdapter
                    = new HttpCredentialsAdapter(
                    GoogleCredentials.getApplicationDefault().createScoped(BigqueryScopes.all()));

            Bigquery.Builder builder = new Bigquery.Builder(
                    GoogleNetHttpTransport.newTrustedTransport(),
                    GsonFactory.getDefaultInstance(),
                    httpRequest -> {
                        httpCredentialsAdapter.initialize(httpRequest);
                        httpRequest.setThrowExceptionOnExecuteError(false);
                    })
                    .setApplicationName("Trino BigQuery Metastore Plugin");

            config.getBigqueryEndpointUrl().ifPresent(builder::setRootUrl);

            return builder.build();
        } catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException("Failed to initialize Google BigQuery client", e);
        }
    }

    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(BigQueryMetastoreConfig.class);
        binder.bind(BigQueryMetastoreFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder,
                Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class))
                .setDefault()
                .to(BigQueryMetastoreFactory.class)
                .in(Scopes.SINGLETON);

        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(false);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof BigQueryMetastoreModule;
    }

    //TODO Cache?

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    //private static Credentials getCredentials(BigQueryMetastoreConfig config) throws IOException {}

}
