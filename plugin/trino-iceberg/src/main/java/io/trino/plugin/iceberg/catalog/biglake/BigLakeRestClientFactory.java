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
package io.trino.plugin.iceberg.catalog.biglake;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTObjectMapperUtil;
import org.apache.iceberg.rest.RESTUtil;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class BigLakeRestClientFactory
        implements Function<Map<String, String>, RESTClient>
{
    private static Constructor<HTTPClient> getHttpClientConstructor()
            throws NoSuchMethodException
    {
        Class<?>[] constructorArgTypes = new Class<?>[] {
                String.class,                 // uri
                Map.class,                    // baseHeaders
                ObjectMapper.class,           // objectMapper
                HttpRequestInterceptor.class, // requestInterceptor
                Map.class                     // properties
        };

        Constructor<HTTPClient> constructor = HTTPClient.class.getDeclaredConstructor(constructorArgTypes);

        constructor.setAccessible(true);
        return constructor;
    }

    /**
     * This method is called by the RESTSessionCatalog to create the client.
     *
     * @param properties The original catalog properties.
     * @return A configured RESTClient.
     */
    @Override
    public RESTClient apply(Map<String, String> properties)
    {
        GoogleAuthRESTAuthenticator googleInterceptor = new GoogleAuthRESTAuthenticator();
        googleInterceptor.initialize(properties);

        return createClientWithReflection(
                properties,
                googleInterceptor);
    }

    /**
     * A private helper method that creates an HTTPClient instance using reflection
     * to call its private constructor. This allows us to inject a custom interceptor.
     */
    private RESTClient createClientWithReflection(
            Map<String, String> properties,
            HttpRequestInterceptor interceptor)
    {
        try {
            Constructor<HTTPClient> constructor = getHttpClientConstructor();

            String baseUri = RESTUtil.stripTrailingSlash(properties.get(CatalogProperties.URI));
            Map<String, String> baseHeaders = new HashMap<>();
            baseHeaders.put("X-Client-Version", IcebergBuild.fullVersion());
            baseHeaders.put("X-Client-Git-Commit-Short", IcebergBuild.gitCommitShortId());
            baseHeaders.put("x-goog-user-project", properties.get("biglake-projectId"));
            ObjectMapper mapper = RESTObjectMapperUtil.defaultObjectMapper();

            return constructor.newInstance(baseUri, baseHeaders, mapper, interceptor, properties);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create HTTPClient via reflection", e);
        }
    }
}
