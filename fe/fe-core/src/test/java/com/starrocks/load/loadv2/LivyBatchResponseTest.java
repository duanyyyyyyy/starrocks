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

import com.google.gson.Gson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

public class LivyBatchResponseTest {
    private static final Gson GSON = new Gson();

    @Test
    public void testDeserializeSubmitResponse() {
        String json = "{"
                + "\"id\": 42,"
                + "\"state\": \"starting\","
                + "\"appId\": null,"
                + "\"appInfo\": {},"
                + "\"log\": [\"stdout: \", \"stderr: \"]"
                + "}";
        LivyBatchResponse response = GSON.fromJson(json, LivyBatchResponse.class);
        Assertions.assertEquals(42, response.getId());
        Assertions.assertEquals("starting", response.getState());
        Assertions.assertNull(response.getAppId());
        Assertions.assertNotNull(response.getAppInfo());
        Assertions.assertEquals(2, response.getLog().size());
    }

    @Test
    public void testDeserializeRunningResponse() {
        String json = "{"
                + "\"id\": 42,"
                + "\"state\": \"running\","
                + "\"appId\": \"application_1234567890_0001\","
                + "\"appInfo\": {"
                + "  \"sparkUiUrl\": \"http://spark-ui:4040\""
                + "},"
                + "\"log\": []"
                + "}";
        LivyBatchResponse response = GSON.fromJson(json, LivyBatchResponse.class);
        Assertions.assertEquals(42, response.getId());
        Assertions.assertEquals("running", response.getState());
        Assertions.assertEquals("application_1234567890_0001", response.getAppId());
        Assertions.assertEquals("http://spark-ui:4040", response.getAppInfo().get("sparkUiUrl"));
    }

    @Test
    public void testDeserializeSuccessResponse() {
        String json = "{"
                + "\"id\": 42,"
                + "\"state\": \"success\","
                + "\"appId\": \"application_1234567890_0001\","
                + "\"appInfo\": {"
                + "  \"sparkUiUrl\": \"http://spark-ui:4040\""
                + "}"
                + "}";
        LivyBatchResponse response = GSON.fromJson(json, LivyBatchResponse.class);
        Assertions.assertEquals("success", response.getState());
    }

    @Test
    public void testDeserializeDeadResponse() {
        String json = "{"
                + "\"id\": 42,"
                + "\"state\": \"dead\","
                + "\"appId\": \"application_1234567890_0001\","
                + "\"appInfo\": {}"
                + "}";
        LivyBatchResponse response = GSON.fromJson(json, LivyBatchResponse.class);
        Assertions.assertEquals("dead", response.getState());
    }

    @Test
    public void testSetters() {
        LivyBatchResponse response = new LivyBatchResponse();
        response.setId(10);
        response.setState("running");
        response.setAppId("app_123");
        response.setAppInfo(Map.of("sparkUiUrl", "http://test"));
        response.setLog(Arrays.asList("line1", "line2"));

        Assertions.assertEquals(10, response.getId());
        Assertions.assertEquals("running", response.getState());
        Assertions.assertEquals("app_123", response.getAppId());
        Assertions.assertEquals("http://test", response.getAppInfo().get("sparkUiUrl"));
        Assertions.assertEquals(2, response.getLog().size());
    }
}
