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

import com.google.common.collect.Maps;
import com.starrocks.catalog.SparkResource;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.LoadException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.BrokerUtil;
import com.starrocks.common.util.CommandResult;
import com.starrocks.common.util.Util;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.loadv2.etl.EtlJobConfig;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.thrift.TEtlState;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class YarnSparkSubmitterTest {

    private long loadJobId;
    private String label;
    private String resourceName;
    private String broker;
    private String appId;
    private String etlOutputPath;
    private String dppVersion;
    private SparkRepository.SparkArchive archive;

    private final String runningReport = "Application Report :\n" +
            "Application-Id : application_15888888888_0088\n" +
            "Application-Name : label0\n" +
            "Application-Type : SPARK-2.4.1\n" +
            "User : test\n" +
            "Queue : test-queue\n" +
            "Start-Time : 1597654469958\n" +
            "Finish-Time : 0\n" +
            "Progress : 50%\n" +
            "State : RUNNING\n" +
            "Final-State : UNDEFINED\n" +
            "Tracking-URL : http://127.0.0.1:8080/proxy/application_1586619723848_0088/\n" +
            "RPC Port : 40236\n" +
            "AM Host : host-name";

    private final String finishReport = "Application Report :\n" +
            "Application-Id : application_15888888888_0088\n" +
            "Application-Name : label0\n" +
            "Application-Type : SPARK-2.4.1\n" +
            "User : test\n" +
            "Queue : test-queue\n" +
            "Start-Time : 1597654469958\n" +
            "Finish-Time : 1597654801939\n" +
            "Progress : 100%\n" +
            "State : FINISHED\n" +
            "Final-State : SUCCEEDED\n" +
            "Tracking-URL : http://127.0.0.1:8080/proxy/application_1586619723848_0088/\n" +
            "RPC Port : 40236\n" +
            "AM Host : host-name";

    private final String failedReport = "Application Report :\n" +
            "Application-Id : application_15888888888_0088\n" +
            "Application-Name : label0\n" +
            "Application-Type : SPARK-2.4.1\n" +
            "User : test\n" +
            "Queue : test-queue\n" +
            "Start-Time : 1597654469958\n" +
            "Finish-Time : 1597654801939\n" +
            "Progress : 100%\n" +
            "State : FINISHED\n" +
            "Final-State : FAILED\n" +
            "Tracking-URL : http://127.0.0.1:8080/proxy/application_1586619723848_0088/\n" +
            "RPC Port : 40236\n" +
            "AM Host : host-name";

    @BeforeEach
    public void setUp() {
        FeConstants.runningUnitTest = true;
        loadJobId = 1L;
        label = "label0";
        resourceName = "spark0";
        broker = "broker0";
        appId = "application_15888888888_0088";
        etlOutputPath = "hdfs://127.0.0.1:10000/tmp/starrocks/100/label/101";
        dppVersion = Config.spark_dpp_version;
        String remoteArchivePath = etlOutputPath + "/__repository__/__archive_" + dppVersion;
        archive = new SparkRepository.SparkArchive(remoteArchivePath, dppVersion);
        archive.libraries.add(new SparkRepository
                .SparkLibrary("", "", SparkRepository.SparkLibrary.LibType.DPP, 0L));
        archive.libraries.add(new SparkRepository
                .SparkLibrary("", "", SparkRepository.SparkLibrary.LibType.SPARK2X, 0L));
    }

    @Test
    public void testSubmitSuccess(@Mocked BrokerUtil brokerUtil, @Mocked SparkLauncher launcher,
                                  @Injectable Process process,
                                  @Mocked SparkLoadAppHandle handle) throws IOException, LoadException {
        new Expectations() {
            {
                launcher.launch();
                result = process;
                handle.getAppId();
                result = appId;
                handle.getState();
                result = SparkLoadAppHandle.State.RUNNING;
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        new Expectations(resource) {
            {
                resource.prepareArchive();
                result = archive;
            }
        };

        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");

        EtlJobConfig etlJobConfig = new EtlJobConfig(Maps.newHashMap(), etlOutputPath, label, null);
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        SparkSubmitParam param = new SparkSubmitParam(loadJobId, label, etlJobConfig,
                resource, brokerDesc, handle, Config.spark_load_submit_timeout_second);

        YarnSparkSubmitter submitter = new YarnSparkSubmitter();
        SparkSubmitResult result = submitter.submit(param);

        Assertions.assertEquals(appId, result.getAppId());
        Assertions.assertNotNull(result.getHandle());
    }

    @Test
    public void testSubmitFailed(@Mocked BrokerUtil brokerUtil, @Mocked SparkLauncher launcher,
                                 @Injectable Process process,
                                 @Mocked SparkLoadAppHandle handle) {
        Assertions.assertThrows(LoadException.class, () -> {
            new Expectations() {
                {
                    launcher.launch();
                    result = process;
                    handle.getAppId();
                    result = appId;
                    handle.getState();
                    result = SparkLoadAppHandle.State.FAILED;
                }
            };

            SparkResource resource = new SparkResource(resourceName);
            new Expectations(resource) {
                {
                    resource.prepareArchive();
                    result = archive;
                }
            };

            Map<String, String> sparkConfigs = resource.getSparkConfigs();
            sparkConfigs.put("spark.master", "yarn");
            sparkConfigs.put("spark.submit.deployMode", "cluster");
            sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");

            EtlJobConfig etlJobConfig = new EtlJobConfig(Maps.newHashMap(), etlOutputPath, label, null);
            BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
            SparkSubmitParam param = new SparkSubmitParam(loadJobId, label, etlJobConfig,
                    resource, brokerDesc, handle, Config.spark_load_submit_timeout_second);

            YarnSparkSubmitter submitter = new YarnSparkSubmitter();
            submitter.submit(param);
        });
    }

    @Test
    public void testGetStatusRunning(@Mocked BrokerUtil brokerUtil, @Mocked Util util,
                                     @Mocked CommandResult commandResult,
                                     @Mocked SparkYarnConfigFiles sparkYarnConfigFiles) throws StarRocksException {
        new Expectations() {
            {
                sparkYarnConfigFiles.prepare();
                sparkYarnConfigFiles.getConfigDir();
                result = "./yarn_config";
                commandResult.getReturnCode();
                result = 0;
                commandResult.getStdout();
                result = runningReport;
            }
        };

        new Expectations() {
            {
                Util.executeCommand(anyString, (String[]) any, anyLong);
                minTimes = 0;
                result = commandResult;
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        new Expectations(resource) {
            {
                resource.getYarnClientPath();
                result = Config.yarn_client_path;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        YarnSparkSubmitter submitter = new YarnSparkSubmitter();
        EtlStatus status = submitter.getStatus(appId, loadJobId, etlOutputPath, resource, brokerDesc);

        Assertions.assertEquals(TEtlState.RUNNING, status.getState());
        Assertions.assertEquals(50, status.getProgress());
    }

    @Test
    public void testGetStatusFinished(@Mocked BrokerUtil brokerUtil, @Mocked Util util,
                                      @Mocked CommandResult commandResult,
                                      @Mocked SparkYarnConfigFiles sparkYarnConfigFiles) throws StarRocksException {
        new Expectations() {
            {
                sparkYarnConfigFiles.prepare();
                sparkYarnConfigFiles.getConfigDir();
                result = "./yarn_config";
                commandResult.getReturnCode();
                result = 0;
                commandResult.getStdout();
                result = finishReport;
            }
        };

        new Expectations() {
            {
                Util.executeCommand(anyString, (String[]) any, anyLong);
                minTimes = 0;
                result = commandResult;
                BrokerUtil.readFile(anyString, (BrokerDesc) any);
                result = "{'normal_rows': 100, 'abnormal_rows': 0}";
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        new Expectations(resource) {
            {
                resource.getYarnClientPath();
                result = Config.yarn_client_path;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        YarnSparkSubmitter submitter = new YarnSparkSubmitter();
        EtlStatus status = submitter.getStatus(appId, loadJobId, etlOutputPath, resource, brokerDesc);

        Assertions.assertEquals(TEtlState.FINISHED, status.getState());
        Assertions.assertEquals(100, status.getProgress());
        Assertions.assertNotNull(status.getDppResult());
        Assertions.assertEquals(100, status.getDppResult().normalRows);
    }

    @Test
    public void testGetStatusFailed(@Mocked BrokerUtil brokerUtil, @Mocked Util util,
                                    @Mocked CommandResult commandResult,
                                    @Mocked SparkYarnConfigFiles sparkYarnConfigFiles) throws StarRocksException {
        new Expectations() {
            {
                sparkYarnConfigFiles.prepare();
                sparkYarnConfigFiles.getConfigDir();
                result = "./yarn_config";
                commandResult.getReturnCode();
                result = 0;
                commandResult.getStdout();
                result = failedReport;
            }
        };

        new Expectations() {
            {
                Util.executeCommand(anyString, (String[]) any, anyLong);
                minTimes = 0;
                result = commandResult;
                BrokerUtil.readFile(anyString, (BrokerDesc) any);
                result = "{'normal_rows': 0, 'abnormal_rows': 10, 'failed_reason': 'data error'}";
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        new Expectations(resource) {
            {
                resource.getYarnClientPath();
                result = Config.yarn_client_path;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        YarnSparkSubmitter submitter = new YarnSparkSubmitter();
        EtlStatus status = submitter.getStatus(appId, loadJobId, etlOutputPath, resource, brokerDesc);

        Assertions.assertEquals(TEtlState.CANCELLED, status.getState());
        Assertions.assertEquals("data error", status.getDppResult().failedReason);
    }

    @Test
    public void testKillYarnApp(@Mocked Util util, @Mocked CommandResult commandResult,
                                @Mocked SparkYarnConfigFiles sparkYarnConfigFiles) throws StarRocksException {
        new Expectations() {
            {
                sparkYarnConfigFiles.prepare();
                sparkYarnConfigFiles.getConfigDir();
                result = "./yarn_config";
                commandResult.getReturnCode();
                result = 0;
            }
        };

        new Expectations() {
            {
                Util.executeCommand(anyString, (String[]) any, anyLong);
                minTimes = 0;
                result = commandResult;
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        new Expectations(resource) {
            {
                resource.getYarnClientPath();
                result = Config.yarn_client_path;
            }
        };

        YarnSparkSubmitter submitter = new YarnSparkSubmitter();
        submitter.kill(appId, loadJobId, resource);
    }

    @Test
    public void testKillEmptyAppId() throws StarRocksException {
        SparkResource resource = new SparkResource(resourceName);
        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");
        sparkConfigs.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");

        YarnSparkSubmitter submitter = new YarnSparkSubmitter();
        // should not throw with empty appId
        submitter.kill("", loadJobId, resource);
        submitter.kill(null, loadJobId, resource);
    }

    @Test
    public void testGetStatusNonYarnMode() throws StarRocksException {
        SparkResource resource = new SparkResource(resourceName);
        // set spark.master to standalone so isYarnMaster() returns false
        resource.getSparkConfigs().put("spark.master", "local");

        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        YarnSparkSubmitter submitter = new YarnSparkSubmitter();
        EtlStatus status = submitter.getStatus(appId, loadJobId, etlOutputPath, resource, brokerDesc);

        Assertions.assertEquals(TEtlState.CANCELLED, status.getState());
        Assertions.assertTrue(status.getFailMsg().contains("non-yarn mode"));
    }
}
