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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class LivyClientTest {

    @Test
    public void testSubmitBatch() throws LoadException {
        String responseJson = "{\"id\": 10, \"state\": \"starting\", \"appId\": null, \"appInfo\": {}}";
        mockLivyClientConnection(200, responseJson);

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
        mockLivyClientConnection(200, responseJson);

        LivyClient client = new LivyClient("http://livy:8998/");
        LivyBatchResponse response = client.getBatchStatus(10);
        Assertions.assertEquals("running", response.getState());
        Assertions.assertEquals("app_123", response.getAppId());
    }

    @Test
    public void testDeleteBatch() throws LoadException {
        mockLivyClientConnection(200, "");
        LivyClient client = new LivyClient("http://livy:8998");
        client.deleteBatch(10);
    }

    @Test
    public void testHttpError() {
        mockLivyClientConnection(500, "{\"msg\": \"error\"}");
        LivyClient client = new LivyClient("http://livy:8998");
        Assertions.assertThrows(LoadException.class, () -> client.submitBatch(new HashMap<>()));
    }

    @Test
    public void testIOException() {
        new MockUp<LivyClient>() {
            @Mock
            HttpURLConnection openConnection(String urlStr) throws IOException {
                throw new IOException("connection refused");
            }
        };

        LivyClient client = new LivyClient("http://livy:8998");
        Assertions.assertThrows(LoadException.class, () -> client.submitBatch(new HashMap<>()));
    }

    @Test
    public void testReadBodyNullErrorStream() {
        new MockUp<LivyClient>() {
            @Mock
            HttpURLConnection openConnection(String urlStr) {
                return new StubHttpURLConnection(500, "") {
                    @Override
                    public InputStream getErrorStream() {
                        return null;
                    }
                };
            }
        };

        LivyClient client = new LivyClient("http://livy:8998");
        // should not NPE, should throw LoadException with empty body
        Assertions.assertThrows(LoadException.class, () -> client.submitBatch(new HashMap<>()));
    }

    private void mockLivyClientConnection(int code, String body) {
        new MockUp<LivyClient>() {
            @Mock
            HttpURLConnection openConnection(String urlStr) {
                return new StubHttpURLConnection(code, body);
            }
        };
    }

    /**
     * A simple HttpURLConnection stub for testing.
     */
    private static class StubHttpURLConnection extends HttpURLConnection {
        private final int responseCode;
        private final String body;

        StubHttpURLConnection(int responseCode, String body) {
            super(null);
            this.responseCode = responseCode;
            this.body = body;
        }

        @Override
        public int getResponseCode() {
            return responseCode;
        }

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public InputStream getErrorStream() {
            return new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public OutputStream getOutputStream() {
            return new ByteArrayOutputStream();
        }

        @Override
        public void setRequestProperty(String key, String value) {
            // no-op
        }

        @Override
        public void disconnect() { }

        @Override
        public boolean usingProxy() {
            return false;
        }

        @Override
        public void connect() { }
    }
}
