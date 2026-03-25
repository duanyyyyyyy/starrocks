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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.starrocks.catalog.SparkResource;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.LoadException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.BrokerUtil;
import com.starrocks.common.util.CommandResult;
import com.starrocks.common.util.Util;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.loadv2.SparkLoadAppHandle.State;
import com.starrocks.load.loadv2.dpp.DppResult;
import com.starrocks.load.loadv2.etl.EtlJobConfig;
import com.starrocks.load.loadv2.etl.SparkEtlJob;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.thrift.TEtlState;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * YARN mode implementation of SparkSubmitter.
 * Submits Spark ETL jobs via spark-submit and monitors via YARN CLI.
 * This is a pure refactor of the existing logic from SparkEtlJobHandler.
 */
public class YarnSparkSubmitter implements SparkSubmitter {
    private static final Logger LOG = LogManager.getLogger(YarnSparkSubmitter.class);

    private static final String CONFIG_FILE_NAME = "jobconfig.json";
    private static final String JOB_CONFIG_DIR = "configs";
    private static final String ETL_JOB_NAME = "starrocks__%s";
    private static final String LAUNCHER_LOG = "spark_launcher_%s_%s.log";
    private static final long EXEC_CMD_TIMEOUT_MS = 30000L;
    private static final String YARN_STATUS_CMD = "%s --config %s application -status %s";
    private static final String YARN_KILL_CMD = "%s --config %s application -kill %s";

    @Override
    public SparkSubmitResult submit(SparkSubmitParam param) throws LoadException {
        long loadJobId = param.getLoadJobId();
        String loadLabel = param.getLoadLabel();
        EtlJobConfig etlJobConfig = param.getEtlJobConfig();
        SparkResource resource = param.getResource();
        BrokerDesc brokerDesc = param.getBrokerDesc();
        SparkLoadAppHandle handle = param.getHandle();
        long sparkLoadSubmitTimeout = param.getSubmitTimeoutMs();

        // init local dir
        if (!FeConstants.runningUnitTest) {
            SparkEtlJobHandler.initLocalDir();
        }

        // prepare dpp archive
        SparkRepository.SparkArchive archive = resource.prepareArchive();
        SparkRepository.SparkLibrary dppLibrary = archive.getDppLibrary();
        SparkRepository.SparkLibrary spark2xLibrary = archive.getSpark2xLibrary();

        // spark home
        String sparkHome = Config.spark_home_default_dir;
        // etl config path
        String configsHdfsDir = etlJobConfig.outputPath + "/" + JOB_CONFIG_DIR + "/";
        // etl config json path
        String jobConfigHdfsPath = configsHdfsDir + CONFIG_FILE_NAME;
        // spark submit app resource path
        String appResourceHdfsPath = dppLibrary.remotePath;
        // spark yarn archive path
        String jobArchiveHdfsPath = spark2xLibrary.remotePath;
        // spark yarn stage dir
        String jobStageHdfsPath = resource.getWorkingDir();
        // spark launcher log path
        String logFilePath = Config.spark_launcher_log_dir + "/" + String.format(LAUNCHER_LOG, loadJobId, loadLabel);

        // update archive and stage configs here
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        if (Strings.isNullOrEmpty(sparkConfigs.get("spark.yarn.archive"))) {
            sparkConfigs.put("spark.yarn.archive", jobArchiveHdfsPath);
        }
        if (Strings.isNullOrEmpty(sparkConfigs.get("spark.yarn.stage.dir"))) {
            sparkConfigs.put("spark.yarn.stage.dir", jobStageHdfsPath);
        }

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

        SparkLauncher launcher = new SparkLauncher();
        launcher.setMaster(resource.getMaster())
                .setDeployMode(resource.getDeployMode().name().toLowerCase())
                .setAppResource(appResourceHdfsPath)
                .setMainClass(SparkEtlJob.class.getCanonicalName())
                .setAppName(String.format(ETL_JOB_NAME, loadLabel))
                .setSparkHome(sparkHome)
                .addAppArgs(jobConfigHdfsPath)
                .redirectError();

        // spark configs
        for (Map.Entry<String, String> entry : resource.getSparkConfigs().entrySet()) {
            launcher.setConf(entry.getKey(), entry.getValue());
        }

        // start app
        State state = null;
        String appId = null;
        String logPath = null;
        String errMsg = "start spark app failed. error: ";
        try {
            Process process = launcher.launch();
            handle.setProcess(process);
            if (!FeConstants.runningUnitTest) {
                SparkLauncherMonitor.LogMonitor logMonitor = SparkLauncherMonitor.createLogMonitor(handle);
                logMonitor.setSubmitTimeoutMs(sparkLoadSubmitTimeout);
                logMonitor.setRedirectLogPath(logFilePath);
                logMonitor.start();
                try {
                    logMonitor.join();
                } catch (InterruptedException e) {
                    logMonitor.interrupt();
                    throw new LoadException(errMsg + e.getMessage());
                }
            }
            appId = handle.getAppId();
            state = handle.getState();
            logPath = handle.getLogPath();
        } catch (IOException e) {
            LOG.warn(errMsg, e);
            throw new LoadException(errMsg + e.getMessage());
        }

        if (fromSparkState(state) == TEtlState.CANCELLED) {
            if (state == State.KILLED) {
                try {
                    killYarnApplication(appId, loadJobId, resource);
                } catch (StarRocksException e) {
                    LOG.warn(errMsg, e);
                }
            }
            throw new LoadException(
                    errMsg + "spark app state: " + state.toString() + ", loadJobId:" + loadJobId + ", logPath:" +
                            logPath);
        }

        if (appId == null) {
            throw new LoadException(errMsg + "Waiting too much time to get appId from handle. spark app state: "
                    + state.toString() + ", loadJobId:" + loadJobId);
        }

        return new SparkSubmitResult(appId, handle);
    }

    @Override
    public EtlStatus getStatus(String appId, long loadJobId, String etlOutputPath,
                                SparkResource resource, BrokerDesc brokerDesc) throws StarRocksException {
        EtlStatus status = new EtlStatus();

        Preconditions.checkState(appId != null && !appId.isEmpty());
        if (resource.isYarnMaster()) {
            // prepare yarn config
            String configDir = resource.prepareYarnConfig();
            // yarn client path
            String yarnClient = resource.getYarnClientPath();
            // command: yarn --config configDir application -status appId
            String yarnStatusCmd = String.format(YARN_STATUS_CMD, yarnClient, configDir, appId);
            LOG.info(yarnStatusCmd);
            String[] envp = {"LC_ALL=" + Config.locale, "JAVA_HOME=" + System.getProperty("java.home")};
            CommandResult result = Util.executeCommand(yarnStatusCmd, envp, EXEC_CMD_TIMEOUT_MS);
            if (result.getReturnCode() != 0) {
                String stderr = result.getStderr();
                if (stderr != null && stderr.contains("doesn't exist in RM")) {
                    LOG.warn("spark application not found. spark app id: {}, load job id: {}, stderr: {}",
                            appId, loadJobId, stderr);
                    status.setState(TEtlState.CANCELLED);
                    status.setFailMsg("spark application not found");
                    return status;
                }

                LOG.warn("yarn application status failed. spark app id: {}, load job id: {}, timeout: {}" +
                                ", return code: {}, stderr: {}, stdout: {}",
                        appId, loadJobId, EXEC_CMD_TIMEOUT_MS, result.getReturnCode(), stderr, result.getStdout());
                throw new LoadException("yarn application status failed. error: " + stderr);
            }
            ApplicationReport report = new YarnApplicationReport(result.getStdout()).getReport();
            LOG.info("yarn application -status {}. load job id: {}, output: {}, report: {}",
                    appId, loadJobId, result.getStdout(), report);
            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus faStatus = report.getFinalApplicationStatus();
            status.setState(fromYarnState(state, faStatus));
            if (status.getState() == TEtlState.CANCELLED) {
                if (state == YarnApplicationState.FINISHED) {
                    status.setFailMsg("spark app state: " + faStatus.toString());
                } else {
                    status.setFailMsg("yarn app state: " + state.toString());
                }
            }
            status.setTrackingUrl(report.getTrackingUrl());
            status.setProgress((int) (report.getProgress() * 100));
        } else {
            // non-yarn mode: state not available without handle
            status.setFailMsg("non-yarn mode status query not supported via YarnSparkSubmitter");
            status.setState(TEtlState.CANCELLED);
            return status;
        }

        if (status.getState() == TEtlState.FINISHED || status.getState() == TEtlState.CANCELLED) {
            readDppResult(status, etlOutputPath, brokerDesc);
        }

        return status;
    }

    @Override
    public void kill(String appId, long loadJobId, SparkResource resource) throws StarRocksException {
        if (resource.isYarnMaster()) {
            killYarnApplication(appId, loadJobId, resource);
        }
    }

    private void killYarnApplication(String appId, long loadJobId, SparkResource resource)
            throws StarRocksException {
        if (!resource.isYarnMaster()) {
            return;
        }
        if (Strings.isNullOrEmpty(appId)) {
            LOG.warn("app id is null, kill yarn application fail");
            return;
        }
        String configDir = resource.prepareYarnConfig();
        String yarnClient = resource.getYarnClientPath();
        String yarnKillCmd = String.format(YARN_KILL_CMD, yarnClient, configDir, appId);
        LOG.info(yarnKillCmd);
        String[] envp = {"LC_ALL=" + Config.locale, "JAVA_HOME=" + System.getProperty("java.home")};
        CommandResult result = Util.executeCommand(yarnKillCmd, envp, EXEC_CMD_TIMEOUT_MS);
        LOG.info("yarn application -kill {}, output: {}", appId, result.getStdout());
        if (result.getReturnCode() != 0) {
            String stderr = result.getStderr();
            LOG.warn("yarn application kill failed. app id: {}, load job id: {}, msg: {}", appId, loadJobId,
                    stderr);
        }
    }

    static void readDppResult(EtlStatus status, String etlOutputPath, BrokerDesc brokerDesc) {
        String dppResultFilePath = EtlJobConfig.getDppResultFilePath(etlOutputPath);
        try {
            byte[] data;
            if (brokerDesc.hasBroker()) {
                data = BrokerUtil.readFile(dppResultFilePath, brokerDesc);
            } else {
                data = HdfsUtil.readFile(dppResultFilePath, brokerDesc.getProperties());
            }
            String dppResultStr = new String(data, StandardCharsets.UTF_8);
            DppResult dppResult = new Gson().fromJson(dppResultStr, DppResult.class);
            if (dppResult != null) {
                status.setDppResult(dppResult);
                if (status.getState() == TEtlState.CANCELLED && !Strings.isNullOrEmpty(dppResult.failedReason)) {
                    status.setFailMsg(dppResult.failedReason);
                }
            }
        } catch (StarRocksException | JsonSyntaxException e) {
            LOG.warn("read broker file failed. path: {}", dppResultFilePath, e);
        }
    }

    private TEtlState fromYarnState(YarnApplicationState state, FinalApplicationStatus faStatus) {
        switch (state) {
            case FINISHED:
                if (faStatus == FinalApplicationStatus.SUCCEEDED) {
                    return TEtlState.FINISHED;
                } else {
                    return TEtlState.CANCELLED;
                }
            case FAILED:
            case KILLED:
                return TEtlState.CANCELLED;
            default:
                return TEtlState.RUNNING;
        }
    }

    private TEtlState fromSparkState(State state) {
        switch (state) {
            case FINISHED:
                return TEtlState.FINISHED;
            case FAILED:
            case KILLED:
            case LOST:
                return TEtlState.CANCELLED;
            default:
                return TEtlState.RUNNING;
        }
    }
}
