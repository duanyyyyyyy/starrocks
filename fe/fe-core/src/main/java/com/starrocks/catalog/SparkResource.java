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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/SparkResource.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.LoadException;
import com.starrocks.common.Pair;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.load.loadv2.SparkRepository;
import com.starrocks.load.loadv2.SparkYarnConfigFiles;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.ResourceDesc;

import java.io.File;
import java.util.Map;

/**
 * Spark resource for etl or query.
 * working_dir and broker[.xxx] are optional and used in spark ETL.
 * working_dir is used to store ETL intermediate files and broker is used to read the intermediate files by BE.
 * <p>
 * Two submission modes are supported, controlled by "spark.mode":
 * <p>
 * 1. YARN mode (default): submits via spark-submit, requires spark.master, spark.submit.deployMode, YARN RM configs.
 * <p>
 * CREATE EXTERNAL RESOURCE "spark0"
 * PROPERTIES
 * (
 * "type" = "spark",
 * "spark.master" = "yarn",
 * "spark.submit.deployMode" = "cluster",
 * "spark.hadoop.yarn.resourcemanager.address" = "127.0.0.1:9999",
 * "spark.hadoop.fs.defaultFS" = "hdfs://127.0.0.1:10000",
 * "working_dir" = "hdfs://127.0.0.1:10000/tmp/starrocks",
 * "broker" = "broker0"
 * );
 * <p>
 * 2. Livy mode: submits via Apache Livy Batch REST API, only requires livy.url.
 * <p>
 * CREATE EXTERNAL RESOURCE "spark_livy"
 * PROPERTIES
 * (
 * "type" = "spark",
 * "spark.mode" = "livy",
 * "livy.url" = "http://livy-server:8998",
 * "working_dir" = "hdfs://127.0.0.1:10000/tmp/starrocks",
 * "broker" = "broker0"
 * );
 * <p>
 * DROP RESOURCE "spark0";
 */
public class SparkResource extends Resource {
    private static final String SPARK_MASTER = "spark.master";
    private static final String SPARK_SUBMIT_DEPLOY_MODE = "spark.submit.deployMode";
    private static final String SPARK_MODE = "spark.mode";
    private static final String WORKING_DIR = "working_dir";
    private static final String BROKER = "broker";
    private static final String YARN_MASTER = "yarn";
    private static final String SPARK_CONFIG_PREFIX = "spark.";
    private static final String LIVY_URL = "livy.url";
    private static final String LIVY_USERNAME = "livy.username";
    private static final String LIVY_PASSWORD = "livy.password";
    private static final String BROKER_PROPERTY_PREFIX = "broker.";
    // spark uses hadoop configs in the form of spark.hadoop.*
    private static final String SPARK_HADOOP_CONFIG_PREFIX = "spark.hadoop.";
    private static final String SPARK_YARN_RESOURCE_MANAGER_ADDRESS = "spark.hadoop.yarn.resourcemanager.address";
    private static final String SPARK_YARN_RESOURCE_MANAGER_HA_ENABLED = "spark.hadoop.yarn.resourcemanager.ha.enabled";
    private static final String SPARK_YARN_RESOURCE_MANAGER_HA_RMIDS = "spark.hadoop.yarn.resourcemanager.ha.rm-ids";
    private static final String SPARK_FS_DEFAULT_FS = "spark.hadoop.fs.defaultFS";
    private static final String YARN_RESOURCE_MANAGER_ADDRESS = "yarn.resourcemanager.address";

    private static final String YARN_RESOURCE_MANAGER_ADDRESS_FOMART = "spark.hadoop.yarn.resourcemanager.address.%s";
    private static final String YARN_RESOURCE_MANAGER_HOSTNAME_FORMAT = "spark.hadoop.yarn.resourcemanager.hostname.%s";

    public enum SparkMode {
        YARN,
        LIVY;

        public static SparkMode fromString(String mode) {
            for (SparkMode m : SparkMode.values()) {
                if (m.name().equalsIgnoreCase(mode)) {
                    return m;
                }
            }
            return null;
        }
    }

    public enum DeployMode {
        CLUSTER,
        CLIENT;

        public static DeployMode fromString(String deployMode) {
            for (DeployMode mode : DeployMode.values()) {
                if (mode.name().equalsIgnoreCase(deployMode)) {
                    return mode;
                }
            }
            return null;
        }
    }

    @SerializedName(value = "sparkMode")
    private SparkMode sparkMode;
    @SerializedName(value = "sparkConfigs")
    private Map<String, String> sparkConfigs;
    @SerializedName(value = "workingDir")
    private String workingDir;
    @SerializedName(value = "broker")
    private String broker;
    // broker username and password
    @SerializedName(value = "brokerProperties")
    private Map<String, String> brokerProperties;
    @SerializedName(value = "hasBroker")
    private boolean hasBroker;
    @SerializedName(value = "livyUrl")
    private String livyUrl;
    @SerializedName(value = "livyUsername")
    private String livyUsername;
    @SerializedName(value = "livyPassword")
    private String livyPassword;

    public SparkResource(String name) {
        this(name, Maps.newHashMap(), null, null, Maps.newHashMap(), false);
    }

    private SparkResource(String name, Map<String, String> sparkConfigs, String workingDir, String broker,
                          Map<String, String> brokerProperties, boolean hasBroker) {
        super(name, ResourceType.SPARK);
        this.sparkConfigs = sparkConfigs;
        this.workingDir = workingDir;
        this.broker = broker;
        this.brokerProperties = brokerProperties;
        this.hasBroker = hasBroker;
    }

    public String getMaster() {
        return sparkConfigs.get(SPARK_MASTER);
    }

    public DeployMode getDeployMode() {
        return DeployMode.fromString(sparkConfigs.get(SPARK_SUBMIT_DEPLOY_MODE));
    }

    public String getWorkingDir() {
        return workingDir;
    }

    public String getBroker() {
        return broker;
    }

    public boolean hasBroker() {
        return hasBroker;
    }

    public Map<String, String> getBrokerPropertiesWithoutPrefix() {
        Map<String, String> properties = Maps.newHashMap();
        for (Map.Entry<String, String> entry : brokerProperties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(BROKER_PROPERTY_PREFIX)) {
                properties.put(key.substring(key.indexOf(".") + 1), entry.getValue());
            }
        }
        return properties;
    }

    public Map<String, String> getSparkConfigs() {
        return sparkConfigs;
    }

    public Pair<String, String> getYarnResourcemanagerAddressPair() {
        return Pair.create(YARN_RESOURCE_MANAGER_ADDRESS, sparkConfigs.get(SPARK_YARN_RESOURCE_MANAGER_ADDRESS));
    }

    public SparkResource getCopiedResource() {
        SparkResource copied = new SparkResource(name, Maps.newHashMap(sparkConfigs), workingDir, broker,
                brokerProperties, hasBroker);
        copied.sparkMode = this.sparkMode;
        copied.livyUrl = this.livyUrl;
        copied.livyUsername = this.livyUsername;
        copied.livyPassword = this.livyPassword;
        return copied;
    }

    // Each SparkResource has and only has one SparkRepository.
    // This method get the remote archive which matches the dpp version from remote repository
    public synchronized SparkRepository.SparkArchive prepareArchive() throws LoadException {
        String remoteRepositoryPath = workingDir + "/" + GlobalStateMgr.getCurrentState().getNodeMgr().getClusterId()
                + "/" + SparkRepository.REPOSITORY_DIR + name;
        BrokerDesc brokerDesc;
        if (hasBroker) {
            brokerDesc = new BrokerDesc(broker, getBrokerPropertiesWithoutPrefix());
        } else {
            brokerDesc = new BrokerDesc(getBrokerPropertiesWithoutPrefix());
        }
        SparkRepository repository = new SparkRepository(remoteRepositoryPath, brokerDesc);
        // This checks and uploads the remote archive.
        repository.prepare();
        SparkRepository.SparkArchive archive = repository.getCurrentArchive();
        // Normally, an archive should contain a DPP library and a SPARK library
        Preconditions.checkState(archive.libraries.size() == 2);
        SparkRepository.SparkLibrary dppLibrary = archive.getDppLibrary();
        SparkRepository.SparkLibrary spark2xLibrary = archive.getSpark2xLibrary();
        if (dppLibrary == null || spark2xLibrary == null) {
            throw new LoadException("failed to get libraries from remote archive");
        }
        return archive;
    }

    // Each SparkResource has and only has one yarn config to run yarn command
    // This method will write all the configuration start with "spark.hadoop." into config files in a specific directory
    public synchronized String prepareYarnConfig() throws LoadException {
        SparkYarnConfigFiles yarnConfigFiles = new SparkYarnConfigFiles(name, getSparkHadoopConfig(sparkConfigs));
        yarnConfigFiles.prepare();
        return yarnConfigFiles.getConfigDir();
    }

    public String getYarnClientPath() throws LoadException {
        String yarnClientPath = Config.yarn_client_path;
        File file = new File(yarnClientPath);
        if (!file.exists() || !file.isFile()) {
            throw new LoadException("yarn client does not exist in path: " + yarnClientPath);
        }
        return yarnClientPath;
    }

    public SparkMode getSparkMode() {
        return sparkMode != null ? sparkMode : SparkMode.YARN;
    }

    public boolean isYarnMode() {
        return getSparkMode() == SparkMode.YARN;
    }

    public boolean isYarnMaster() {
        if (isYarnMode()) {
            return getMaster() != null && getMaster().equalsIgnoreCase(YARN_MASTER);
        }
        return false;
    }

    public boolean isLivyMode() {
        return getSparkMode() == SparkMode.LIVY;
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

    public void update(ResourceDesc resourceDesc) throws DdlException {
        Preconditions.checkState(name.equals(resourceDesc.getName()));

        Map<String, String> properties = resourceDesc.getProperties();
        if (properties == null) {
            return;
        }

        // update spark configs
        if (properties.containsKey(SPARK_MODE)) {
            throw new DdlException("Cannot change spark.mode");
        }
        if (properties.containsKey(SPARK_MASTER)) {
            throw new DdlException("Cannot change spark master");
        }
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

        // parse spark.mode (default: yarn)
        String modeStr = properties.get(SPARK_MODE);
        if (modeStr != null) {
            sparkMode = SparkMode.fromString(modeStr);
            if (sparkMode == null) {
                throw new DdlException("Unknown spark.mode: " + modeStr + ". Supported: yarn, livy");
            }
        } else {
            sparkMode = SparkMode.YARN;
        }

        // get spark configs
        sparkConfigs = getSparkConfig(properties);

        if (isLivyMode()) {
            // Livy mode: requires livy.url, no spark.master / spark.submit.deployMode needed
            livyUrl = properties.get(LIVY_URL);
            if (Strings.isNullOrEmpty(livyUrl)) {
                throw new DdlException("Missing " + LIVY_URL + " in livy mode");
            }
            livyUsername = properties.get(LIVY_USERNAME);
            livyPassword = properties.get(LIVY_PASSWORD);
        } else {
            // YARN mode: requires spark.master, spark.submit.deployMode
            if (getMaster() == null) {
                throw new DdlException("Missing " + SPARK_MASTER + " in properties");
            }
            String deployModeStr = sparkConfigs.get(SPARK_SUBMIT_DEPLOY_MODE);
            if (deployModeStr != null) {
                DeployMode deployMode = DeployMode.fromString(deployModeStr);
                if (deployMode == null) {
                    throw new DdlException("Unknown deploy mode: " + deployModeStr);
                }
            } else {
                throw new DdlException("Missing " + SPARK_SUBMIT_DEPLOY_MODE + " in properties");
            }

            if (isYarnMaster()) {
                if (!sparkConfigs.containsKey(SPARK_FS_DEFAULT_FS)) {
                    throw new DdlException("Missing " + SPARK_FS_DEFAULT_FS + " in yarn master");
                }

                String resourceManagerHaEnabled = sparkConfigs.get(SPARK_YARN_RESOURCE_MANAGER_HA_ENABLED);
                if (sparkConfigs.containsKey(SPARK_YARN_RESOURCE_MANAGER_ADDRESS)) {
                    // standalone yarn resource manager
                } else if (resourceManagerHaEnabled != null && resourceManagerHaEnabled.equalsIgnoreCase("true")) {
                    if (!sparkConfigs.containsKey(SPARK_YARN_RESOURCE_MANAGER_HA_RMIDS)) {
                        throw new DdlException(
                                "Missing " + SPARK_YARN_RESOURCE_MANAGER_HA_RMIDS + " in yarn master");
                    }

                    String[] haRmIds = sparkConfigs.get(SPARK_YARN_RESOURCE_MANAGER_HA_RMIDS).split(",");
                    for (String haRmId : haRmIds) {
                        String addressKey = String.format(YARN_RESOURCE_MANAGER_ADDRESS_FOMART, haRmId);
                        String hostnameKey = String.format(YARN_RESOURCE_MANAGER_HOSTNAME_FORMAT, haRmId);
                        if (!sparkConfigs.containsKey(addressKey) && !sparkConfigs.containsKey(hostnameKey)) {
                            throw new DdlException(
                                    "Missing " + addressKey + " or " + hostnameKey + " in yarn master");
                        }
                    }
                } else {
                    throw new DdlException("Missing " + SPARK_YARN_RESOURCE_MANAGER_ADDRESS +
                            " or resource manager HA configs in yarn master");
                }
            }
        }

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

    private Map<String, String> getSparkConfig(Map<String, String> properties) {
        Map<String, String> sparkConfig = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(SPARK_CONFIG_PREFIX) && !entry.getKey().equals(SPARK_MODE)) {
                sparkConfig.put(entry.getKey(), entry.getValue());
            }
        }
        return sparkConfig;
    }

    private Map<String, String> getSparkHadoopConfig(Map<String, String> properties) {
        Map<String, String> sparkConfig = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(SPARK_HADOOP_CONFIG_PREFIX)) {
                sparkConfig.put(entry.getKey(), entry.getValue());
            }
        }
        return sparkConfig;
    }

    private Map<String, String> getBrokerProperties(Map<String, String> properties) {
        Map<String, String> brokerProperties = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(BROKER_PROPERTY_PREFIX)) {
                brokerProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return brokerProperties;
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        result.addRow(Lists.newArrayList(name, lowerCaseType, SPARK_MODE,
                getSparkMode().name().toLowerCase()));
        for (Map.Entry<String, String> entry : sparkConfigs.entrySet()) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
        if (workingDir != null) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, SparkResource.WORKING_DIR, workingDir));
        }
        if (broker != null) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, SparkResource.BROKER, broker));
        }
        if (livyUrl != null) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, SparkResource.LIVY_URL, livyUrl));
        }
        for (Map.Entry<String, String> entry : brokerProperties.entrySet()) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }
}
