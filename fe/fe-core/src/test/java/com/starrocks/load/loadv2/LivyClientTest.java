// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.load.loadv2;

import com.starrocks.common.LoadException;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class LivyClientTest {

    @Test
    public void testSubmitBatch() throws LoadException {
        String responseJson = "{\"id\": 10, \"state\": \"starting\", \"appId\": null, \"appInfo\": {}}";
        mockUrlConnection(200, responseJson);

        LivyClient client = new LivyClient("http://livy:8998");
        Map<String, Object> request = new HashMap<>();
        request.put("file", "hdfs://dpp.jar");

        LivyBatchResponse response = client.submitBatch(request);
        Assertions.assertEquals(10, response.getId());
        Assertions.assertEquals("starting", response.getState());
    }

    @Test
    public void testGetBatchStatus() throws LoadException {
        String responseJson = "{\"id\": 10, \"state\": \"running\", \"appId\": \"app_123\","
                + " \"appInfo\": {\"sparkUiUrl\": \"http://ui\"}}";
        mockUrlConnection(200, responseJson);

        LivyClient client = new LivyClient("http://livy:8998/");
        LivyBatchResponse response = client.getBatchStatus(10);
        Assertions.assertEquals("running", response.getState());
        Assertions.assertEquals("app_123", response.getAppId());
    }

    @Test
    public void testDeleteBatch() throws LoadException {
        mockUrlConnection(200, "");
        LivyClient client = new LivyClient("http://livy:8998");
        client.deleteBatch(10);
    }

    @Test
    public void testHttpError() {
        mockUrlConnection(500, "{\"msg\": \"error\"}");
        LivyClient client = new LivyClient("http://livy:8998");
        Assertions.assertThrows(LoadException.class, () -> client.submitBatch(new HashMap<>()));
    }

    @Test
    public void testIOException() {
        new MockUp<java.net.URL>() {
            @Mock
            public HttpURLConnection openConnection() throws IOException {
                throw new IOException("connection refused");
            }
        };

        LivyClient client = new LivyClient("http://livy:8998");
        Assertions.assertThrows(LoadException.class, () -> client.submitBatch(new HashMap<>()));
    }

    @Test
    public void testBasicAuth() throws LoadException {
        String responseJson = "{\"id\": 1, \"state\": \"running\", \"appId\": null, \"appInfo\": {}}";
        AtomicReference<String> capturedAuth = new AtomicReference<>();
        mockUrlConnectionWithAuthCapture(200, responseJson, capturedAuth);

        LivyClient client = new LivyClient("http://livy:8998", "admin", "secret");
        client.getBatchStatus(1);

        String expected = "Basic " + Base64.getEncoder().encodeToString(
                "admin:secret".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals(expected, capturedAuth.get());
    }

    @Test
    public void testNoAuthWhenUsernameNull() throws LoadException {
        String responseJson = "{\"id\": 2, \"state\": \"running\", \"appId\": null, \"appInfo\": {}}";
        AtomicReference<String> capturedAuth = new AtomicReference<>();
        mockUrlConnectionWithAuthCapture(200, responseJson, capturedAuth);

        LivyClient client = new LivyClient("http://livy:8998", null, null);
        client.getBatchStatus(2);
        Assertions.assertNull(capturedAuth.get());
    }

    private void mockUrlConnection(int code, String body) {
        new MockUp<java.net.URL>() {
            @Mock
            public HttpURLConnection openConnection() {
                return new MockUp<HttpURLConnection>() {
                    @Mock public void setRequestMethod(String m) { }
                    @Mock public void setRequestProperty(String k, String v) { }
                    @Mock public void setDoOutput(boolean b) { }
                    @Mock public void setConnectTimeout(int t) { }
                    @Mock public void setReadTimeout(int t) { }
                    @Mock public java.io.OutputStream getOutputStream() { return new ByteArrayOutputStream(); }
                    @Mock public int getResponseCode() { return code; }
                    @Mock public java.io.InputStream getInputStream() {
                        return new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
                    }
                    @Mock public java.io.InputStream getErrorStream() {
                        return new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
                    }
                }.getMockInstance();
            }
        };
    }

    private void mockUrlConnectionWithAuthCapture(int code, String body, AtomicReference<String> authCapture) {
        new MockUp<java.net.URL>() {
            @Mock
            public HttpURLConnection openConnection() {
                return new MockUp<HttpURLConnection>() {
                    @Mock public void setRequestMethod(String m) { }
                    @Mock public void setRequestProperty(String k, String v) {
                        if ("Authorization".equals(k)) {
                            authCapture.set(v);
                        }
                    }
                    @Mock public void setDoOutput(boolean b) { }
                    @Mock public void setConnectTimeout(int t) { }
                    @Mock public void setReadTimeout(int t) { }
                    @Mock public java.io.OutputStream getOutputStream() { return new ByteArrayOutputStream(); }
                    @Mock public int getResponseCode() { return code; }
                    @Mock public java.io.InputStream getInputStream() {
                        return new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
                    }
                }.getMockInstance();
            }
        };
    }
}
