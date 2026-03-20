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
import com.starrocks.common.LoadException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * HTTP client wrapper for Livy Batch REST API.
 */
public class LivyClient {
    private static final Logger LOG = LogManager.getLogger(LivyClient.class);
    private static final Gson GSON = new Gson();
    private static final int CONNECT_TIMEOUT_MS = 5000;
    private static final int READ_TIMEOUT_MS = 30000;

    private final String livyUrl;
    private final String authHeader;

    public LivyClient(String livyUrl) {
        this(livyUrl, null, null);
    }

    public LivyClient(String livyUrl, String username, String password) {
        // remove trailing slash
        this.livyUrl = livyUrl.endsWith("/") ? livyUrl.substring(0, livyUrl.length() - 1) : livyUrl;
        if (username != null && !username.isEmpty()) {
            String credentials = username + ":" + (password != null ? password : "");
            this.authHeader = "Basic " + Base64.getEncoder().encodeToString(
                    credentials.getBytes(StandardCharsets.UTF_8));
        } else {
            this.authHeader = null;
        }
    }

    /**
     * POST /batches — submit a new batch job.
     */
    public LivyBatchResponse submitBatch(Object requestBody) throws LoadException {
        String json = GSON.toJson(requestBody);
        String responseJson = httpPost(livyUrl + "/batches", json);
        return GSON.fromJson(responseJson, LivyBatchResponse.class);
    }

    /**
     * GET /batches/{batchId} — get batch status.
     */
    public LivyBatchResponse getBatchStatus(int batchId) throws LoadException {
        String responseJson = httpGet(livyUrl + "/batches/" + batchId);
        return GSON.fromJson(responseJson, LivyBatchResponse.class);
    }

    /**
     * DELETE /batches/{batchId} — kill a batch job.
     */
    public void deleteBatch(int batchId) throws LoadException {
        httpDelete(livyUrl + "/batches/" + batchId);
    }

    private String httpPost(String urlStr, String jsonBody) throws LoadException {
        try {
            HttpURLConnection conn = openConnection(urlStr);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
            }
            return readResponse(conn, urlStr);
        } catch (IOException e) {
            throw new LoadException("Livy POST request failed. url: " + urlStr + ", error: " + e.getMessage());
        }
    }

    private String httpGet(String urlStr) throws LoadException {
        try {
            HttpURLConnection conn = openConnection(urlStr);
            conn.setRequestMethod("GET");
            return readResponse(conn, urlStr);
        } catch (IOException e) {
            throw new LoadException("Livy GET request failed. url: " + urlStr + ", error: " + e.getMessage());
        }
    }

    private void httpDelete(String urlStr) throws LoadException {
        try {
            HttpURLConnection conn = openConnection(urlStr);
            conn.setRequestMethod("DELETE");
            int code = conn.getResponseCode();
            if (code < 200 || code >= 300) {
                String body = readBody(conn);
                LOG.warn("Livy DELETE failed. url: {}, code: {}, body: {}", urlStr, code, body);
                throw new LoadException("Livy DELETE failed. url: " + urlStr + ", code: " + code);
            }
        } catch (IOException e) {
            throw new LoadException("Livy DELETE request failed. url: " + urlStr + ", error: " + e.getMessage());
        }
    }

    private HttpURLConnection openConnection(String urlStr) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(READ_TIMEOUT_MS);
        if (authHeader != null) {
            conn.setRequestProperty("Authorization", authHeader);
        }
        return conn;
    }

    private String readResponse(HttpURLConnection conn, String urlStr) throws LoadException, IOException {
        int code = conn.getResponseCode();
        if (code < 200 || code >= 300) {
            String body = readBody(conn);
            LOG.warn("Livy request failed. url: {}, code: {}, body: {}", urlStr, code, body);
            throw new LoadException("Livy request failed. url: " + urlStr + ", code: " + code + ", body: " + body);
        }
        return readBody(conn);
    }

    private String readBody(HttpURLConnection conn) {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        conn.getResponseCode() >= 400 ? conn.getErrorStream() : conn.getInputStream(),
                        StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            LOG.warn("Failed to read Livy response body", e);
        }
        return sb.toString();
    }
}
