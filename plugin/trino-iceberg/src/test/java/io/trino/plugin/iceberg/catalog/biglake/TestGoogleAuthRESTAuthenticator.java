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
 * You may may obtain a copy of the License at
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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableMap;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestGoogleAuthRESTAuthenticator
{
    private GoogleAuthRESTAuthenticator authenticator;

    @Mock
    private GoogleCredentials mockCredentials;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS) // For chained calls like getAccessToken().getTokenValue()
    private AccessToken mockAccessToken;

    @Mock
    private HttpRequest mockHttpRequest;

    @Mock
    private EntityDetails mockEntityDetails;

    @Mock
    private HttpContext mockHttpContext;

    private MockedStatic<GoogleCredentials> mockedStaticCredentials;

    @TempDir
    private Path tempDir; // JUnit 5 temporary directory

    @BeforeEach
    public void setUp()
    {
        authenticator = new GoogleAuthRESTAuthenticator();
        // Mock the static methods of GoogleCredentials
        mockedStaticCredentials = mockStatic(GoogleCredentials.class);
    }

    @AfterEach
    public void tearDown()
    {
        mockedStaticCredentials.close();
    }

    private File createDummyCredentialsFile(String content)
            throws IOException
    {
        File credentialsFile = tempDir.resolve("dummy-credentials.json").toFile();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(credentialsFile))) {
            writer.write(content);
        }
        return credentialsFile;
    }

    private String getServiceAccountJson()
    {
        return "{\n" +
                "  \"type\": \"service_account\",\n" +
                "  \"project_id\": \"test-project\",\n" +
                "  \"private_key_id\": \"test_key_id\",\n" +
                "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nYOUR_PRIVATE_KEY\\n-----END PRIVATE KEY-----\\n\",\n" +
                "  \"client_email\": \"test@example.com\",\n" +
                "  \"client_id\": \"12345\",\n" +
                "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
                "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
                "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
                "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/test%40example.com\"\n" +
                "}";
    }

    @Test
    public void testInitializeWithValidCredentialsPath()
            throws IOException
    {
        File credentialsFile = createDummyCredentialsFile(getServiceAccountJson());
        Map<String, String> properties = ImmutableMap.of(
                GoogleAuthRESTAuthenticator.CREDENTIALS_PATH_PROPERTY, credentialsFile.getAbsolutePath());
        List<String> expectedScopes = Arrays.asList(GoogleAuthRESTAuthenticator.DEFAULT_SCOPES.split(","));

        mockedStaticCredentials.when(() -> GoogleCredentials.fromStream(any(FileInputStream.class)))
                .thenReturn(mockCredentials);
        when(mockCredentials.createScoped(expectedScopes)).thenReturn(mockCredentials);

        authenticator.initialize(properties);

        mockedStaticCredentials.verify(() -> GoogleCredentials.fromStream(any(FileInputStream.class)));
        verify(mockCredentials).createScoped(expectedScopes);
        mockedStaticCredentials.verify(GoogleCredentials::getApplicationDefault, never());
    }

    @Test
    public void testInitializeWithCustomScopes()
            throws IOException
    {
        File credentialsFile = createDummyCredentialsFile(getServiceAccountJson());
        String customScopes = "scope1,scope2";
        Map<String, String> properties = ImmutableMap.of(
                GoogleAuthRESTAuthenticator.CREDENTIALS_PATH_PROPERTY, credentialsFile.getAbsolutePath(),
                GoogleAuthRESTAuthenticator.SCOPES_PROPERTY, customScopes);
        List<String> expectedScopes = Arrays.asList(customScopes.split(","));

        mockedStaticCredentials.when(() -> GoogleCredentials.fromStream(any(FileInputStream.class)))
                .thenReturn(mockCredentials);
        when(mockCredentials.createScoped(expectedScopes)).thenReturn(mockCredentials);

        authenticator.initialize(properties);

        mockedStaticCredentials.verify(() -> GoogleCredentials.fromStream(any(FileInputStream.class)));
        verify(mockCredentials).createScoped(expectedScopes);
        mockedStaticCredentials.verify(GoogleCredentials::getApplicationDefault, never());
    }

    @Test
    public void testInitializeWithoutCredentialsPath()
            throws IOException
    {
        Map<String, String> properties = Collections.emptyMap();
        List<String> expectedScopes = Arrays.asList(GoogleAuthRESTAuthenticator.DEFAULT_SCOPES.split(","));

        mockedStaticCredentials.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);
        when(mockCredentials.createScoped(expectedScopes)).thenReturn(mockCredentials);

        authenticator.initialize(properties);

        mockedStaticCredentials.verify(GoogleCredentials::getApplicationDefault);
        verify(mockCredentials).createScoped(expectedScopes);
        mockedStaticCredentials.verify(() -> GoogleCredentials.fromStream(any(FileInputStream.class)), never());
    }

    @Test
    public void testInitializeWithEmptyCredentialsPath()
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.of(GoogleAuthRESTAuthenticator.CREDENTIALS_PATH_PROPERTY, "");
        List<String> expectedScopes = Arrays.asList(GoogleAuthRESTAuthenticator.DEFAULT_SCOPES.split(","));

        mockedStaticCredentials.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);
        when(mockCredentials.createScoped(expectedScopes)).thenReturn(mockCredentials);

        authenticator.initialize(properties);

        mockedStaticCredentials.verify(GoogleCredentials::getApplicationDefault);
        verify(mockCredentials).createScoped(expectedScopes);
        mockedStaticCredentials.verify(() -> GoogleCredentials.fromStream(any(FileInputStream.class)), never());
    }

    @Test
    public void testInitializeWithInvalidCredentialsPath()
    {
        Map<String, String> properties = ImmutableMap.of(GoogleAuthRESTAuthenticator.CREDENTIALS_PATH_PROPERTY, "/path/to/nonexistent/file.json");

        assertThatThrownBy(() -> authenticator.initialize(properties))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessage("Failed to load Google credentials")
                .cause()
                .isInstanceOf(IOException.class);
    }

    @Test
    public void testInitializeWithMalformedCredentials()
            throws IOException
    {
        File malformedFile = createDummyCredentialsFile("this is not valid json");
        Map<String, String> properties = ImmutableMap.of(
                GoogleAuthRESTAuthenticator.CREDENTIALS_PATH_PROPERTY, malformedFile.getAbsolutePath());

        mockedStaticCredentials
                .when(() -> GoogleCredentials.fromStream(any(FileInputStream.class)))
                .thenThrow(new IOException("Invalid JSON format"));

        assertThatThrownBy(() -> authenticator.initialize(properties))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessage("Failed to load Google credentials")
                .cause()
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid JSON format");
    }

    @Test
    public void testProcessAddsAuthorizationHeader()
            throws IOException, HttpException
    {
        // Initialize with default credentials
        mockedStaticCredentials.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);
        when(mockCredentials.createScoped(anyList())).thenReturn(mockCredentials);
        authenticator.initialize(Collections.emptyMap());

        String fakeToken = "fake-oauth2-token-abc123";
        when(mockCredentials.getAccessToken()).thenReturn(mockAccessToken);
        when(mockAccessToken.getTokenValue()).thenReturn(fakeToken);

        authenticator.process(mockHttpRequest, mockEntityDetails, mockHttpContext);

        verify(mockCredentials).refreshIfExpired();
        verify(mockCredentials).getAccessToken();
        verify(mockHttpRequest).removeHeaders("Authorization");
        verify(mockHttpRequest).addHeader("Authorization", "Bearer " + fakeToken);
    }

    @Test
    public void testProcessWhenAccessTokenIsNull()
            throws IOException, HttpException
    {
        mockedStaticCredentials.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);
        when(mockCredentials.createScoped(anyList())).thenReturn(mockCredentials);
        authenticator.initialize(Collections.emptyMap());

        when(mockCredentials.getAccessToken()).thenReturn(null);

        authenticator.process(mockHttpRequest, mockEntityDetails, mockHttpContext);

        verify(mockCredentials).refreshIfExpired();
        verify(mockCredentials).getAccessToken();
        verify(mockHttpRequest, never()).removeHeaders("Authorization");
        verify(mockHttpRequest, never()).addHeader(any(String.class), any(String.class));
    }

    @Test
    public void testProcessWhenTokenValueIsNull()
            throws IOException, HttpException
    {
        mockedStaticCredentials.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);
        when(mockCredentials.createScoped(anyList())).thenReturn(mockCredentials);
        authenticator.initialize(Collections.emptyMap());

        when(mockCredentials.getAccessToken()).thenReturn(mockAccessToken);
        when(mockAccessToken.getTokenValue()).thenReturn(null);

        authenticator.process(mockHttpRequest, mockEntityDetails, mockHttpContext);

        verify(mockCredentials).refreshIfExpired();
        verify(mockCredentials).getAccessToken();
        verify(mockHttpRequest, never()).removeHeaders("Authorization");
        verify(mockHttpRequest, never()).addHeader(any(String.class), any(String.class));
    }

    @Test
    public void testProcessWhenRefreshFails()
            throws IOException
    {
        mockedStaticCredentials.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);
        when(mockCredentials.createScoped(anyList())).thenReturn(mockCredentials);
        authenticator.initialize(Collections.emptyMap());

        doThrow(new IOException("Token refresh failed")).when(mockCredentials).refreshIfExpired();

        assertThatThrownBy(() -> authenticator.process(mockHttpRequest, mockEntityDetails, mockHttpContext))
                .isInstanceOf(IOException.class)
                .hasMessage("Token refresh failed");

        verify(mockCredentials).refreshIfExpired();
        verify(mockCredentials, never()).getAccessToken();
        verify(mockHttpRequest, never()).addHeader(any(String.class), any(String.class));
    }
}
