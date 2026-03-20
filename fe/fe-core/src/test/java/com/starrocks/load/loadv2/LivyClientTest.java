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
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class LivyClientTest {

    @Test
    public void testSubmitBatch(@Mocked HttpURLConnection conn) throws LoadException, IOException {
        String responseJson = "{\"id\": 10, \"state\": \"starting\", \"appId\": null, \"appInfo\": {}}";

        new MockUp<URL>() {
            @Mock
            public HttpURLConnection openConnection() {
                return conn;
            }
        };

        new Expectations() {
            {
                conn.getOutputStream();
                result = new ByteArrayOutputStream();
                conn.getResponseCode();
                result = 200;
                conn.getInputStream();
                result = new ByteArrayInputStream(responseJson.getBytes(StandardCharsets.UTF_8));
            }
        };

        LivyClient client = new LivyClient("http://livy:8998");
        Map<String, Object> request = new HashMap<>();
        request.put("file", "hdfs://dpp.jar");
        request.put("className", "com.test.Main");

        LivyBatchResponse response = client.submitBatch(request);
        Assertions.assertEquals(10, response.getId());
        Assertions.assertEquals("starting", response.getState());
    }

    @Test
    public void testGetBatchStatus(@Mocked HttpURLConnection conn) throws LoadException, IOException {
        String responseJson = "{\"id\": 10, \"state\": \"running\", \"appId\": \"app_123\","
                + " \"appInfo\": {\"sparkUiUrl\": \"http://ui\"}}";

        new MockUp<URL>() {
            @Mock
            public HttpURLConnection openConnection() {
                return conn;
            }
        };

        new Expectations() {
            {
                conn.getResponseCode();
                result = 200;
                conn.getInputStream();
                result = new ByteArrayInputStream(responseJson.getBytes(StandardCharsets.UTF_8));
            }
        };

        LivyClient client = new LivyClient("http://livy:8998/");
        LivyBatchResponse response = client.getBatchStatus(10);
        Assertions.assertEquals(10, response.getId());
        Assertions.assertEquals("running", response.getState());
        Assertions.assertEquals("app_123", response.getAppId());
    }

    @Test
    public void testDeleteBatch(@Mocked HttpURLConnection conn) throws LoadException, IOException {
        new MockUp<URL>() {
            @Mock
            public HttpURLConnection openConnection() {
                return conn;
            }
        };

        new Expectations() {
            {
                conn.getResponseCode();
                result = 200;
            }
        };

        LivyClient client = new LivyClient("http://livy:8998");
        client.deleteBatch(10);
    }

    @Test
    public void testSubmitBatchHttpError(@Mocked HttpURLConnection conn) throws IOException {
        new MockUp<URL>() {
            @Mock
            public HttpURLConnection openConnection() {
                return conn;
            }
        };

        new Expectations() {
            {
                conn.getOutputStream();
                result = new ByteArrayOutputStream();
                conn.getResponseCode();
                result = 500;
                conn.getErrorStream();
                result = new ByteArrayInputStream("{\"msg\": \"error\"}".getBytes(StandardCharsets.UTF_8));
            }
        };

        LivyClient client = new LivyClient("http://livy:8998");
        Map<String, Object> request = new HashMap<>();
        Assertions.assertThrows(LoadException.class, () -> client.submitBatch(request));
    }

    @Test
    public void testGetBatchStatusHttpError(@Mocked HttpURLConnection conn) throws IOException {
        new MockUp<URL>() {
            @Mock
            public HttpURLConnection openConnection() {
                return conn;
            }
        };

        new Expectations() {
            {
                conn.getResponseCode();
                result = 404;
                conn.getErrorStream();
                result = new ByteArrayInputStream("{\"msg\": \"not found\"}".getBytes(StandardCharsets.UTF_8));
            }
        };

        LivyClient client = new LivyClient("http://livy:8998");
        Assertions.assertThrows(LoadException.class, () -> client.getBatchStatus(999));
    }

    @Test
    public void testDeleteBatchHttpError(@Mocked HttpURLConnection conn) throws IOException {
        new MockUp<URL>() {
            @Mock
            public HttpURLConnection openConnection() {
                return conn;
            }
        };

        new Expectations() {
            {
                conn.getResponseCode();
                result = 500;
                conn.getErrorStream();
                result = new ByteArrayInputStream("{\"msg\": \"error\"}".getBytes(StandardCharsets.UTF_8));
            }
        };

        LivyClient client = new LivyClient("http://livy:8998");
        Assertions.assertThrows(LoadException.class, () -> client.deleteBatch(10));
    }

    @Test
    public void testSubmitBatchIOException() {
        new MockUp<URL>() {
            @Mock
            public HttpURLConnection openConnection() throws IOException {
                throw new IOException("connection refused");
            }
        };

        LivyClient client = new LivyClient("http://livy:8998");
        Map<String, Object> request = new HashMap<>();
        Assertions.assertThrows(LoadException.class, () -> client.submitBatch(request));
    }

    @Test
    public void testBasicAuth(@Mocked HttpURLConnection conn) throws LoadException, IOException {
        String responseJson = "{\"id\": 1, \"state\": \"running\", \"appId\": null, \"appInfo\": {}}";

        new MockUp<URL>() {
            @Mock
            public HttpURLConnection openConnection() {
                return conn;
            }
        };

        String expectedAuth = "Basic " + Base64.getEncoder().encodeToString(
                "admin:secret".getBytes(StandardCharsets.UTF_8));

        new Expectations() {
            {
                conn.setRequestProperty("Authorization", expectedAuth);
                times = 1;
                conn.getResponseCode();
                result = 200;
                conn.getInputStream();
                result = new ByteArrayInputStream(responseJson.getBytes(StandardCharsets.UTF_8));
            }
        };

        LivyClient client = new LivyClient("http://livy:8998", "admin", "secret");
        LivyBatchResponse response = client.getBatchStatus(1);
        Assertions.assertEquals(1, response.getId());
    }

    @Test
    public void testNoAuthWhenUsernameEmpty(@Mocked HttpURLConnection conn) throws LoadException, IOException {
        String responseJson = "{\"id\": 2, \"state\": \"running\", \"appId\": null, \"appInfo\": {}}";

        new MockUp<URL>() {
            @Mock
            public HttpURLConnection openConnection() {
                return conn;
            }
        };

        new Expectations() {
            {
                conn.setRequestProperty("Authorization", withAny(""));
                times = 0;
                conn.getResponseCode();
                result = 200;
                conn.getInputStream();
                result = new ByteArrayInputStream(responseJson.getBytes(StandardCharsets.UTF_8));
            }
        };

        LivyClient client = new LivyClient("http://livy:8998", null, null);
        LivyBatchResponse response = client.getBatchStatus(2);
        Assertions.assertEquals(2, response.getId());
    }

    @Test
    public void testTrailingSlashHandling(@Mocked HttpURLConnection conn) throws LoadException, IOException {
        String responseJson = "{\"id\": 5, \"state\": \"success\", \"appId\": \"app_1\", \"appInfo\": {}}";

        new MockUp<URL>() {
            @Mock
            public HttpURLConnection openConnection() {
                return conn;
            }
        };

        new Expectations() {
            {
                conn.getResponseCode();
                result = 200;
                conn.getInputStream();
                result = new ByteArrayInputStream(responseJson.getBytes(StandardCharsets.UTF_8));
            }
        };

        LivyClient client = new LivyClient("http://livy:8998/");
        LivyBatchResponse response = client.getBatchStatus(5);
        Assertions.assertEquals(5, response.getId());
    }
}
