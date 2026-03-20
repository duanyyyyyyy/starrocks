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
import com.starrocks.load.EtlStatus;
import com.starrocks.load.loadv2.etl.EtlJobConfig;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.thrift.TEtlState;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class LivySparkSubmitterTest {
    private long loadJobId;
    private String label;
    private String resourceName;
    private String broker;
    private String etlOutputPath;
    private String livyUrl;
    private SparkRepository.SparkArchive archive;

    @BeforeEach
    public void setUp() {
        FeConstants.runningUnitTest = true;
        loadJobId = 1L;
        label = "livy_test_label";
        resourceName = "spark_livy";
        broker = "broker0";
        etlOutputPath = "hdfs://127.0.0.1:10000/tmp/starrocks/100/label/101";
        livyUrl = "http://livy-server:8998";
        String dppVersion = Config.spark_dpp_version;
        String remoteArchivePath = etlOutputPath + "/__repository__/__archive_" + dppVersion;
        archive = new SparkRepository.SparkArchive(remoteArchivePath, dppVersion);
        archive.libraries.add(new SparkRepository
                .SparkLibrary("hdfs://dpp.jar", "dpp.jar", SparkRepository.SparkLibrary.LibType.DPP, 0L));
        archive.libraries.add(new SparkRepository
                .SparkLibrary("hdfs://spark2x.zip", "spark2x.zip", SparkRepository.SparkLibrary.LibType.SPARK2X, 0L));
    }

    @Test
    public void testSubmit(@Mocked BrokerUtil brokerUtil, @Mocked LivyClient livyClient) throws LoadException {
        LivyBatchResponse mockResponse = new LivyBatchResponse();
        mockResponse.setId(42);
        mockResponse.setState("starting");
        mockResponse.setAppId(null);

        new Expectations() {
            {
                new LivyClient(livyUrl);
                result = livyClient;
                livyClient.submitBatch(withNotNull());
                result = mockResponse;
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        new Expectations(resource) {
            {
                resource.prepareArchive();
                result = archive;
                resource.getLivyUrl();
                result = livyUrl;
                resource.getWorkingDir();
                result = "hdfs://127.0.0.1:10000/tmp/starrocks";
            }
        };

        Map<String, String> sparkConfigs = resource.getSparkConfigs();
        sparkConfigs.put("spark.master", "yarn");
        sparkConfigs.put("spark.submit.deployMode", "cluster");

        EtlJobConfig etlJobConfig = new EtlJobConfig(Maps.newHashMap(), etlOutputPath, label, null);
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        SparkLoadAppHandle handle = new SparkLoadAppHandle();

        SparkSubmitParam param = new SparkSubmitParam(loadJobId, label, etlJobConfig,
                resource, brokerDesc, handle, Config.spark_load_submit_timeout_second);

        LivySparkSubmitter submitter = new LivySparkSubmitter();
        SparkSubmitResult result = submitter.submit(param);

        Assertions.assertEquals("42", result.getAppId());
        Assertions.assertNotNull(result.getHandle());
    }

    @Test
    public void testGetStatusRunning(@Mocked LivyClient livyClient) throws StarRocksException {
        LivyBatchResponse mockResponse = new LivyBatchResponse();
        mockResponse.setId(42);
        mockResponse.setState("running");
        mockResponse.setAppId("application_123_001");
        mockResponse.setAppInfo(Map.of("sparkUiUrl", "http://spark-ui:4040"));

        new Expectations() {
            {
                new LivyClient(livyUrl);
                result = livyClient;
                livyClient.getBatchStatus(42);
                result = mockResponse;
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        new Expectations(resource) {
            {
                resource.getLivyUrl();
                result = livyUrl;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        LivySparkSubmitter submitter = new LivySparkSubmitter();
        EtlStatus status = submitter.getStatus("42", loadJobId, etlOutputPath, resource, brokerDesc);

        Assertions.assertEquals(TEtlState.RUNNING, status.getState());
        Assertions.assertEquals("http://spark-ui:4040", status.getTrackingUrl());
    }

    @Test
    public void testGetStatusSuccess(@Mocked LivyClient livyClient, @Mocked BrokerUtil brokerUtil)
            throws StarRocksException {
        LivyBatchResponse mockResponse = new LivyBatchResponse();
        mockResponse.setId(42);
        mockResponse.setState("success");
        mockResponse.setAppId("application_123_001");
        mockResponse.setAppInfo(Map.of("sparkUiUrl", "http://spark-ui:4040"));

        new Expectations() {
            {
                new LivyClient(livyUrl);
                result = livyClient;
                livyClient.getBatchStatus(42);
                result = mockResponse;

                BrokerUtil.readFile(anyString, (BrokerDesc) any);
                result = "{'normal_rows': 100, 'abnormal_rows': 0}";
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        new Expectations(resource) {
            {
                resource.getLivyUrl();
                result = livyUrl;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        LivySparkSubmitter submitter = new LivySparkSubmitter();
        EtlStatus status = submitter.getStatus("42", loadJobId, etlOutputPath, resource, brokerDesc);

        Assertions.assertEquals(TEtlState.FINISHED, status.getState());
        Assertions.assertEquals(100, status.getDppResult().normalRows);
    }

    @Test
    public void testGetStatusDead(@Mocked LivyClient livyClient, @Mocked BrokerUtil brokerUtil)
            throws StarRocksException {
        LivyBatchResponse mockResponse = new LivyBatchResponse();
        mockResponse.setId(42);
        mockResponse.setState("dead");
        mockResponse.setAppId("application_123_001");
        mockResponse.setAppInfo(Map.of());

        new Expectations() {
            {
                new LivyClient(livyUrl);
                result = livyClient;
                livyClient.getBatchStatus(42);
                result = mockResponse;

                BrokerUtil.readFile(anyString, (BrokerDesc) any);
                result = "{'normal_rows': 0, 'abnormal_rows': 0, 'failed_reason': 'OOM'}";
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        new Expectations(resource) {
            {
                resource.getLivyUrl();
                result = livyUrl;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        LivySparkSubmitter submitter = new LivySparkSubmitter();
        EtlStatus status = submitter.getStatus("42", loadJobId, etlOutputPath, resource, brokerDesc);

        Assertions.assertEquals(TEtlState.CANCELLED, status.getState());
        Assertions.assertEquals("OOM", status.getDppResult().failedReason);
    }

    @Test
    public void testGetStatusInvalidBatchId() throws StarRocksException {
        SparkResource resource = new SparkResource(resourceName);
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        LivySparkSubmitter submitter = new LivySparkSubmitter();
        EtlStatus status = submitter.getStatus("not_a_number", loadJobId, etlOutputPath, resource, brokerDesc);

        Assertions.assertEquals(TEtlState.CANCELLED, status.getState());
        Assertions.assertTrue(status.getFailMsg().contains("invalid livy batch id"));
    }

    @Test
    public void testKill(@Mocked LivyClient livyClient) throws StarRocksException {
        new Expectations() {
            {
                new LivyClient(livyUrl);
                result = livyClient;
                livyClient.deleteBatch(42);
            }
        };

        SparkResource resource = new SparkResource(resourceName);
        new Expectations(resource) {
            {
                resource.getLivyUrl();
                result = livyUrl;
            }
        };

        LivySparkSubmitter submitter = new LivySparkSubmitter();
        submitter.kill("42", loadJobId, resource);
    }

    @Test
    public void testKillInvalidBatchId() throws StarRocksException {
        SparkResource resource = new SparkResource(resourceName);
        LivySparkSubmitter submitter = new LivySparkSubmitter();
        // should not throw, just log warning
        submitter.kill("invalid", loadJobId, resource);
    }

    @Test
    public void testLivyStateMapping() throws StarRocksException {
        LivySparkSubmitter submitter = new LivySparkSubmitter();
        SparkResource resource = new SparkResource(resourceName);
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());

        // Test each Livy state mapping via getStatus
        String[] runningStates = {"not_started", "starting", "running", "busy", "idle"};
        for (String state : runningStates) {
            LivyBatchResponse response = new LivyBatchResponse();
            response.setId(1);
            response.setState(state);
            response.setAppInfo(Map.of());

            new MockUp<LivyClient>() {
                @Mock
                public LivyBatchResponse getBatchStatus(int batchId) {
                    return response;
                }
            };

            new Expectations(resource) {
                {
                    resource.getLivyUrl();
                    result = livyUrl;
                }
            };

            EtlStatus status = submitter.getStatus("1", loadJobId, etlOutputPath, resource, brokerDesc);
            Assertions.assertEquals(TEtlState.RUNNING, status.getState(),
                    "Expected RUNNING for Livy state: " + state);
        }
    }
}
