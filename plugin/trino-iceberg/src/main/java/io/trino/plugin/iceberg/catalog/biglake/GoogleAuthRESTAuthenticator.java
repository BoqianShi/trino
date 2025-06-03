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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GoogleAuthRESTAuthenticator
        implements HttpRequestInterceptor
{
    public static final String CREDENTIALS_PATH_PROPERTY = "gcp.auth.credentials-path";
    public static final String SCOPES_PROPERTY = "gcp.auth.scopes";
    public static final String DEFAULT_SCOPES = "https://www.googleapis.com/auth/cloud-platform";
    private static final Logger LOG = LoggerFactory.getLogger(GoogleAuthRESTAuthenticator.class);

    private GoogleCredentials googleCredentials;

    public void initialize(Map<String, String> properties)
    {
        String config = PropertyUtil.propertyAsString(properties, CREDENTIALS_PATH_PROPERTY, "default");
        String credentialsPath = properties.get(CREDENTIALS_PATH_PROPERTY);
        String scopesString = properties.getOrDefault(SCOPES_PROPERTY, DEFAULT_SCOPES);
        List<String> scopes = Arrays.asList(scopesString.split(","));
        try {
            if (credentialsPath != null && !credentialsPath.isEmpty()) {
                LOG.info("Using Google credentials from path: {}", credentialsPath);
                try (FileInputStream credentialsStream = new FileInputStream(credentialsPath)) {
                    this.googleCredentials =
                            GoogleCredentials.fromStream(credentialsStream).createScoped(scopes);
                }
            }
            else {
                LOG.info("Using Application Default Credentials with scopes: {}", scopesString);
                this.googleCredentials = GoogleCredentials.getApplicationDefault().createScoped(scopes);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to load Google credentials", e);
        }
    }

    @Override
    public void process(HttpRequest httpRequest, EntityDetails entityDetails, HttpContext httpContext)
            throws HttpException, IOException
    {
        LOG.debug("Adding Google Oauth2 token to request");
        googleCredentials.refreshIfExpired();
        AccessToken accessToken = googleCredentials.getAccessToken();
        if (accessToken != null && accessToken.getTokenValue() != null) {
            httpRequest.removeHeaders("Authorization");
            httpRequest.addHeader("Authorization", "Bearer " + accessToken.getTokenValue());
        }
    }
}
