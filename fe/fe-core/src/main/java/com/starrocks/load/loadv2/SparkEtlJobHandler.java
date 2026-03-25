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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/SparkEtlJobHandler.java

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

package com.starrocks.load.loadv2;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.SparkResource;
import com.starrocks.common.Config;
import com.starrocks.common.LoadException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.BrokerUtil;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.loadv2.etl.EtlJobConfig;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * SparkEtlJobHandler is the orchestrator for Spark ETL operations.
 * It delegates submission, status query, and kill operations to a SparkSubmitter implementation
 * (YarnSparkSubmitter or LivySparkSubmitter) based on the SparkResource configuration.
 *
 * Responsibilities retained here (not submission-mode specific):
 * 1. delete etl output path
 * 2. get spark etl file paths
 * 3. init local directories
 */
public class SparkEtlJobHandler {
    private static final Logger LOG = LogManager.getLogger(SparkEtlJobHandler.class);

    SparkSubmitter createSubmitter(SparkResource resource) {
        if (resource.isLivyMode()) {
            return new LivySparkSubmitter();
        }
        return new YarnSparkSubmitter();
    }

    public void submitEtlJob(long loadJobId, String loadLabel, EtlJobConfig etlJobConfig, SparkResource resource,
                             BrokerDesc brokerDesc, SparkLoadAppHandle handle, SparkPendingTaskAttachment attachment,
                             Long sparkLoadSubmitTimeout)
            throws LoadException {
        // delete outputPath
        deleteEtlOutputPath(etlJobConfig.outputPath, brokerDesc);

        SparkSubmitter submitter = createSubmitter(resource);
        SparkSubmitParam param = new SparkSubmitParam(loadJobId, loadLabel, etlJobConfig,
                resource, brokerDesc, handle, sparkLoadSubmitTimeout);
        SparkSubmitResult result = submitter.submit(param);

        // success
        attachment.setAppId(result.getAppId());
        attachment.setHandle(result.getHandle());
    }

    public EtlStatus getEtlJobStatus(SparkLoadAppHandle handle, String appId, long loadJobId, String etlOutputPath,
                                     SparkResource resource, BrokerDesc brokerDesc) throws StarRocksException {
        SparkSubmitter submitter = createSubmitter(resource);
        EtlStatus status = submitter.getStatus(appId, loadJobId, etlOutputPath, resource, brokerDesc);

        // prefer tracking URL from handle if available (YARN mode sets URL via spark launcher)
        if (handle != null && handle.getUrl() != null) {
            status.setTrackingUrl(handle.getUrl());
        }

        return status;
    }

    public void killEtlJob(SparkLoadAppHandle handle, String appId, long loadJobId, SparkResource resource)
            throws StarRocksException {
        // For YARN mode, the appId may be empty when the load job is in PENDING phase.
        // In this case, try to get appId from handle, or kill the launcher process directly.
        if (resource.isLivyMode()) {
            // Livy mode: kill via REST API
            SparkSubmitter submitter = createSubmitter(resource);
            submitter.kill(appId, loadJobId, resource);
            return;
        }

        if (resource.isYarnMaster()) {
            if (Strings.isNullOrEmpty(appId)) {
                appId = handle != null ? handle.getAppId() : null;
                if (Strings.isNullOrEmpty(appId)) {
                    if (handle != null) {
                        handle.kill();
                    }
                    return;
                }
            }
        } else {
            // standalone spark mode: just stop handle
            if (handle != null) {
                handle.stop();
            }
            return;
        }

        SparkSubmitter submitter = createSubmitter(resource);
        submitter.kill(appId, loadJobId, resource);
    }

    public Map<String, Long> getEtlFilePaths(String outputPath, BrokerDesc brokerDesc) throws Exception {
        Map<String, Long> filePathToSize = Maps.newHashMap();

        List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
        String etlFilePaths = outputPath + "/*";
        try {
            if (brokerDesc.hasBroker()) {
                BrokerUtil.parseFile(etlFilePaths, brokerDesc, fileStatuses);
            } else {
                HdfsUtil.parseFile(etlFilePaths, brokerDesc.getProperties(), fileStatuses);
            }
        } catch (StarRocksException e) {
            throw new Exception(e);
        }

        for (TBrokerFileStatus fstatus : fileStatuses) {
            if (fstatus.isDir) {
                continue;
            }
            filePathToSize.put(fstatus.getPath(), fstatus.getSize());
        }
        LOG.debug("get spark etl file paths. files map: {}", filePathToSize);

        return filePathToSize;
    }

    public static synchronized void initLocalDir() {
        String logDir = Config.spark_launcher_log_dir;
        File file = new File(logDir);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    public void deleteEtlOutputPath(String outputPath, BrokerDesc brokerDesc) {
        try {
            if (brokerDesc.hasBroker()) {
                BrokerUtil.deletePath(outputPath, brokerDesc);
            } else {
                HdfsUtil.deletePath(outputPath, brokerDesc.getProperties());
            }
            LOG.info("delete path success. path: {}", outputPath);
        } catch (StarRocksException e) {
            LOG.warn("delete path failed. path: {}", outputPath, e);
        }
    }

}
