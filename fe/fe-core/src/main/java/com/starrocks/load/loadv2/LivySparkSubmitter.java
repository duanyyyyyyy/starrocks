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

import com.google.common.base.Strings;
import com.starrocks.catalog.LivyResource;
import com.starrocks.catalog.SparkResource;
import com.starrocks.common.LoadException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.BrokerUtil;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.loadv2.etl.EtlJobConfig;
import com.starrocks.load.loadv2.etl.SparkEtlJob;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.thrift.TEtlState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Livy mode implementation of SparkSubmitter.
 * Submits Spark ETL jobs via Livy Batch REST API and monitors via Livy status API.
 */
public class LivySparkSubmitter implements SparkSubmitter {
    private static final Logger LOG = LogManager.getLogger(LivySparkSubmitter.class);

    private static final String CONFIG_FILE_NAME = "jobconfig.json";
    private static final String JOB_CONFIG_DIR = "configs";
    private static final String ETL_JOB_NAME = "starrocks__%s";

    @Override
    public SparkSubmitResult submit(SparkSubmitParam param) throws LoadException {
        long loadJobId = param.getLoadJobId();
        String loadLabel = param.getLoadLabel();
        EtlJobConfig etlJobConfig = param.getEtlJobConfig();
        LivyResource resource = (LivyResource) param.getResource();
        BrokerDesc brokerDesc = param.getBrokerDesc();

        // 1. prepare archive (reuse existing logic)
        SparkRepository.SparkArchive archive = resource.prepareArchive();
        SparkRepository.SparkLibrary dppLibrary = archive.getDppLibrary();
        SparkRepository.SparkLibrary spark2xLibrary = archive.getSpark2xLibrary();

        String dppJarPath = dppLibrary.remotePath;
        String sparkArchivePath = spark2xLibrary.remotePath;

        // 2. write jobconfig.json to HDFS
        String configsHdfsDir = etlJobConfig.outputPath + "/" + JOB_CONFIG_DIR + "/";
        String jobConfigHdfsPath = configsHdfsDir + CONFIG_FILE_NAME;
        try {
            byte[] configData = etlJobConfig.configToJson().getBytes(StandardCharsets.UTF_8);
            if (brokerDesc.hasBroker()) {
                BrokerUtil.writeFile(configData, jobConfigHdfsPath, brokerDesc);
            } else {
                HdfsUtil.writeFile(configData, jobConfigHdfsPath, brokerDesc.getProperties());
            }
        } catch (StarRocksException e) {
            throw new LoadException(e.getMessage());
        }

        // 3. build Livy batch request
        Map<String, Object> batchRequest = new HashMap<>();
        batchRequest.put("file", dppJarPath);
        batchRequest.put("className", SparkEtlJob.class.getCanonicalName());
        batchRequest.put("args", Collections.singletonList(jobConfigHdfsPath));
        batchRequest.put("name", String.format(ETL_JOB_NAME, loadLabel));

        // build spark conf
        Map<String, String> sparkConf = new HashMap<>(resource.getSparkConfigs());
        if (Strings.isNullOrEmpty(sparkConf.get("spark.yarn.archive"))) {
            sparkConf.put("spark.yarn.archive", sparkArchivePath);
        }
        if (Strings.isNullOrEmpty(sparkConf.get("spark.yarn.stage.dir"))) {
            sparkConf.put("spark.yarn.stage.dir", resource.getWorkingDir());
        }
        batchRequest.put("conf", sparkConf);

        // extract queue from spark conf if present
        String queue = sparkConf.get("spark.yarn.queue");
        if (!Strings.isNullOrEmpty(queue)) {
            batchRequest.put("queue", queue);
        }

        // 4. POST /batches
        LivyClient client = new LivyClient(resource.getLivyUrl(), resource.getLivyUsername(),
                resource.getLivyPassword());
        LivyBatchResponse response = client.submitBatch(batchRequest);

        LOG.info("submitted livy batch. loadJobId: {}, label: {}, batchId: {}, state: {}",
                loadJobId, loadLabel, response.getId(), response.getState());

        // 5. batchId as appId
        String batchId = String.valueOf(response.getId());
        SparkLoadAppHandle handle = param.getHandle();
        if (response.getAppId() != null) {
            handle.setAppId(response.getAppId());
        }
        handle.setState(mapLivyStateToHandleState(response.getState()));

        return new SparkSubmitResult(batchId, handle);
    }

    @Override
    public EtlStatus getStatus(String appId, long loadJobId, String etlOutputPath,
                                SparkResource resource, BrokerDesc brokerDesc) throws StarRocksException {
        LivyResource livyResource = (LivyResource) resource;
        EtlStatus status = new EtlStatus();

        int batchId;
        try {
            batchId = Integer.parseInt(appId);
        } catch (NumberFormatException e) {
            status.setFailMsg("invalid livy batch id: " + appId);
            status.setState(TEtlState.CANCELLED);
            return status;
        }

        LivyClient client = new LivyClient(livyResource.getLivyUrl(), livyResource.getLivyUsername(),
                livyResource.getLivyPassword());
        LivyBatchResponse response;
        try {
            response = client.getBatchStatus(batchId);
        } catch (LoadException e) {
            throw new StarRocksException("Failed to get livy batch status. batchId: " + batchId, e);
        }

        status.setState(mapLivyStateToEtl(response.getState()));
        if (response.getAppInfo() != null) {
            String sparkUiUrl = response.getAppInfo().get("sparkUiUrl");
            if (!Strings.isNullOrEmpty(sparkUiUrl)) {
                status.setTrackingUrl(sparkUiUrl);
            }
        }

        if (status.getState() == TEtlState.CANCELLED) {
            status.setFailMsg("Livy batch state: " + response.getState());
        }

        LOG.info("livy batch status. batchId: {}, loadJobId: {}, state: {}, yarnAppId: {}",
                batchId, loadJobId, response.getState(), response.getAppId());

        // read dpp result
        if (status.getState() == TEtlState.FINISHED || status.getState() == TEtlState.CANCELLED) {
            YarnSparkSubmitter.readDppResult(status, etlOutputPath, brokerDesc);
        }

        return status;
    }

    @Override
    public void kill(String appId, long loadJobId, SparkResource resource) throws StarRocksException {
        LivyResource livyResource = (LivyResource) resource;
        int batchId;
        try {
            batchId = Integer.parseInt(appId);
        } catch (NumberFormatException e) {
            LOG.warn("invalid livy batch id: {}, loadJobId: {}", appId, loadJobId);
            return;
        }

        LivyClient client = new LivyClient(livyResource.getLivyUrl(), livyResource.getLivyUsername(),
                livyResource.getLivyPassword());
        try {
            client.deleteBatch(batchId);
        } catch (LoadException e) {
            throw new StarRocksException("Failed to kill livy batch. batchId: " + batchId, e);
        }
        LOG.info("killed livy batch. batchId: {}, loadJobId: {}", batchId, loadJobId);
    }

    private TEtlState mapLivyStateToEtl(String livyState) {
        if (livyState == null) {
            return TEtlState.RUNNING;
        }
        switch (livyState) {
            case "success":
                return TEtlState.FINISHED;
            case "dead":
            case "killed":
            case "error":
                return TEtlState.CANCELLED;
            default:
                // not_started, starting, running, busy, idle
                return TEtlState.RUNNING;
        }
    }

    private SparkLoadAppHandle.State mapLivyStateToHandleState(String livyState) {
        if (livyState == null) {
            return SparkLoadAppHandle.State.UNKNOWN;
        }
        switch (livyState) {
            case "success":
                return SparkLoadAppHandle.State.FINISHED;
            case "dead":
            case "error":
                return SparkLoadAppHandle.State.FAILED;
            case "killed":
                return SparkLoadAppHandle.State.KILLED;
            case "running":
                return SparkLoadAppHandle.State.RUNNING;
            case "starting":
            case "not_started":
                return SparkLoadAppHandle.State.SUBMITTED;
            default:
                return SparkLoadAppHandle.State.UNKNOWN;
        }
    }
}
