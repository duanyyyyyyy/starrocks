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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ResourceDesc;

import java.util.Map;

/**
 * Livy-based Spark resource for ETL.
 * Submits Spark jobs via Apache Livy Batch REST API instead of spark-submit.
 * <p>
 * Example:
 * CREATE EXTERNAL RESOURCE "spark_livy"
 * PROPERTIES
 * (
 * "type" = "spark_livy",
 * "livy.url" = "http://livy-server:8998",
 * "working_dir" = "hdfs://127.0.0.1:10000/tmp/starrocks",
 * "broker" = "broker0"
 * );
 */
public class LivyResource extends SparkResource {
    private static final String LIVY_URL = "livy.url";
    private static final String LIVY_USERNAME = "livy.username";
    private static final String LIVY_PASSWORD = "livy.password";

    @SerializedName(value = "livyUrl")
    private String livyUrl;
    @SerializedName(value = "livyUsername")
    private String livyUsername;
    @SerializedName(value = "livyPassword")
    private String livyPassword;

    public LivyResource(String name) {
        super(name, ResourceType.SPARK_LIVY, Maps.newHashMap(), null, null, Maps.newHashMap(), false);
    }

    private LivyResource(String name, Map<String, String> sparkConfigs, String workingDir, String broker,
                          Map<String, String> brokerProperties, boolean hasBroker,
                          String livyUrl, String livyUsername, String livyPassword) {
        super(name, ResourceType.SPARK_LIVY, sparkConfigs, workingDir, broker, brokerProperties, hasBroker);
        this.livyUrl = livyUrl;
        this.livyUsername = livyUsername;
        this.livyPassword = livyPassword;
    }

    @Override
    public boolean isLivyMode() {
        return true;
    }

    public String getLivyUrl() {
        return livyUrl;
    }

    public String getLivyUsername() {
        return livyUsername;
    }

    public String getLivyPassword() {
        return livyPassword;
    }

    @Override
    public SparkResource getCopiedResource() {
        return new LivyResource(name, Maps.newHashMap(sparkConfigs), workingDir, broker,
                brokerProperties, hasBroker, livyUrl, livyUsername, livyPassword);
    }

    @Override
    public boolean isYarnMaster() {
        return false;
    }

    @Override
    public void update(ResourceDesc resourceDesc) throws DdlException {
        Preconditions.checkState(name.equals(resourceDesc.getName()));

        Map<String, String> properties = resourceDesc.getProperties();
        if (properties == null) {
            return;
        }

        // update spark configs (pass-through to Livy)
        sparkConfigs.putAll(getSparkConfig(properties));

        // update working dir and broker
        if (properties.containsKey(WORKING_DIR)) {
            workingDir = properties.get(WORKING_DIR);
        }
        if (properties.containsKey(BROKER)) {
            broker = properties.get(BROKER);
            hasBroker = true;
        }
        brokerProperties.putAll(getBrokerProperties(properties));

        // update livy properties
        if (properties.containsKey(LIVY_URL)) {
            livyUrl = properties.get(LIVY_URL);
        }
        if (properties.containsKey(LIVY_USERNAME)) {
            livyUsername = properties.get(LIVY_USERNAME);
        }
        if (properties.containsKey(LIVY_PASSWORD)) {
            livyPassword = properties.get(LIVY_PASSWORD);
        }
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);

        // livy.url is required
        livyUrl = properties.get(LIVY_URL);
        if (Strings.isNullOrEmpty(livyUrl)) {
            throw new DdlException("Missing " + LIVY_URL + " in properties");
        }
        livyUsername = properties.get(LIVY_USERNAME);
        livyPassword = properties.get(LIVY_PASSWORD);

        // optional spark configs (pass-through to Livy batch request)
        sparkConfigs = getSparkConfig(properties);

        // check working dir and broker
        workingDir = properties.get(WORKING_DIR);
        if (properties.containsKey(BROKER)) {
            hasBroker = true;
            broker = properties.get(BROKER);
            if ((workingDir == null && broker != null) || (workingDir != null && broker == null)) {
                throw new DdlException("working_dir and broker should be assigned at the same time");
            }
        } else {
            hasBroker = false;
        }
        // check broker exist
        if (broker != null && !GlobalStateMgr.getCurrentState().getBrokerMgr().containsBroker(broker)) {
            throw new DdlException("Unknown broker name(" + broker + ")");
        }
        brokerProperties = getBrokerProperties(properties);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        if (livyUrl != null) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, LIVY_URL, livyUrl));
        }
        for (Map.Entry<String, String> entry : sparkConfigs.entrySet()) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
        if (workingDir != null) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, WORKING_DIR, workingDir));
        }
        if (broker != null) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, BROKER, broker));
        }
        for (Map.Entry<String, String> entry : brokerProperties.entrySet()) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }
}
