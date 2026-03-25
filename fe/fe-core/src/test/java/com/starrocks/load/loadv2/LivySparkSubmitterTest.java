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
import com.starrocks.catalog.LivyResource;
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
    private String dppVersion;
    private SparkRepository.SparkArchive archive;

    @BeforeEach
    public void setUp() {
        FeConstants.runningUnitTest = true;
        loadJobId = 1L;
        label = "test_label";
        resourceName = "livy_resource";
        broker = "broker0";
        etlOutputPath = "hdfs://127.0.0.1:10000/tmp/starrocks/100/label/101";
        dppVersion = Config.spark_dpp_version;
        String remoteArchivePath = etlOutputPath + "/__repository__/__archive_" + dppVersion;
        archive = new SparkRepository.SparkArchive(remoteArchivePath, dppVersion);
        archive.libraries.add(new SparkRepository
                .SparkLibrary("", "hdfs://dpp.jar", SparkRepository.SparkLibrary.LibType.DPP, 0L));
        archive.libraries.add(new SparkRepository
                .SparkLibrary("", "hdfs://spark2x.tar.gz", SparkRepository.SparkLibrary.LibType.SPARK2X, 0L));
    }

    @Test
    public void testSubmitSuccess(@Mocked BrokerUtil brokerUtil) throws LoadException {
        LivyBatchResponse mockResponse = new LivyBatchResponse();
        mockResponse.setId(42);
        mockResponse.setState("starting");
        mockResponse.setAppId("application_123_001");

        new MockUp<LivyClient>() {
            @Mock
            public LivyBatchResponse submitBatch(Object requestBody) {
                return mockResponse;
            }
        };

        LivyResource resource = new LivyResource(resourceName);
        new Expectations(resource) {
            {
                resource.prepareArchive();
                result = archive;
                resource.getLivyUrl();
                result = "http://livy:8998";
                resource.getWorkingDir();
                result = "hdfs://127.0.0.1:10000/tmp/starrocks";
            }
        };

        EtlJobConfig etlJobConfig = new EtlJobConfig(Maps.newHashMap(), etlOutputPath, label, null);
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        SparkLoadAppHandle handle = new SparkLoadAppHandle();
        SparkSubmitParam param = new SparkSubmitParam(loadJobId, label, etlJobConfig,
                resource, brokerDesc, handle, 300);

        LivySparkSubmitter submitter = new LivySparkSubmitter();
        SparkSubmitResult result = submitter.submit(param);

        Assertions.assertEquals("42", result.getAppId());
        Assertions.assertEquals("application_123_001", result.getHandle().getAppId());
    }

    @Test
    public void testGetStatusRunning() throws StarRocksException {
        LivyBatchResponse mockResponse = new LivyBatchResponse();
        mockResponse.setId(42);
        mockResponse.setState("running");
        mockResponse.setAppId("application_123_001");
        Map<String, String> appInfo = Maps.newHashMap();
        appInfo.put("sparkUiUrl", "http://spark-ui:4040");
        mockResponse.setAppInfo(appInfo);

        new MockUp<LivyClient>() {
            @Mock
            public LivyBatchResponse getBatchStatus(int batchId) {
                return mockResponse;
            }
        };

        LivyResource resource = new LivyResource(resourceName);
        new Expectations(resource) {
            {
                resource.getLivyUrl();
                result = "http://livy:8998";
            }
        };

        LivySparkSubmitter submitter = new LivySparkSubmitter();
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        EtlStatus status = submitter.getStatus("42", loadJobId, etlOutputPath, resource, brokerDesc);

        Assertions.assertEquals(TEtlState.RUNNING, status.getState());
        Assertions.assertEquals("http://spark-ui:4040", status.getTrackingUrl());
    }

    @Test
    public void testGetStatusSuccess(@Mocked BrokerUtil brokerUtil) throws StarRocksException {
        LivyBatchResponse mockResponse = new LivyBatchResponse();
        mockResponse.setId(42);
        mockResponse.setState("success");

        new MockUp<LivyClient>() {
            @Mock
            public LivyBatchResponse getBatchStatus(int batchId) {
                return mockResponse;
            }
        };

        LivyResource resource = new LivyResource(resourceName);
        new Expectations(resource) {
            {
                resource.getLivyUrl();
                result = "http://livy:8998";
            }
        };

        LivySparkSubmitter submitter = new LivySparkSubmitter();
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        EtlStatus status = submitter.getStatus("42", loadJobId, etlOutputPath, resource, brokerDesc);

        Assertions.assertEquals(TEtlState.FINISHED, status.getState());
    }

    @Test
    public void testGetStatusDead(@Mocked BrokerUtil brokerUtil) throws StarRocksException {
        LivyBatchResponse mockResponse = new LivyBatchResponse();
        mockResponse.setId(42);
        mockResponse.setState("dead");

        new MockUp<LivyClient>() {
            @Mock
            public LivyBatchResponse getBatchStatus(int batchId) {
                return mockResponse;
            }
        };

        LivyResource resource = new LivyResource(resourceName);
        new Expectations(resource) {
            {
                resource.getLivyUrl();
                result = "http://livy:8998";
            }
        };

        LivySparkSubmitter submitter = new LivySparkSubmitter();
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        EtlStatus status = submitter.getStatus("42", loadJobId, etlOutputPath, resource, brokerDesc);

        Assertions.assertEquals(TEtlState.CANCELLED, status.getState());
        Assertions.assertTrue(status.getFailMsg().contains("dead"));
    }

    @Test
    public void testGetStatusInvalidBatchId() throws StarRocksException {
        LivyResource resource = new LivyResource(resourceName);
        LivySparkSubmitter submitter = new LivySparkSubmitter();
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());

        EtlStatus status = submitter.getStatus("not_a_number", loadJobId, etlOutputPath, resource, brokerDesc);
        Assertions.assertEquals(TEtlState.CANCELLED, status.getState());
        Assertions.assertTrue(status.getFailMsg().contains("invalid livy batch id"));
    }

    @Test
    public void testKillSuccess() throws StarRocksException {
        new MockUp<LivyClient>() {
            @Mock
            public void deleteBatch(int batchId) {
            }
        };

        LivyResource resource = new LivyResource(resourceName);
        new Expectations(resource) {
            {
                resource.getLivyUrl();
                result = "http://livy:8998";
            }
        };

        LivySparkSubmitter submitter = new LivySparkSubmitter();
        submitter.kill("42", loadJobId, resource);
    }

    @Test
    public void testKillInvalidBatchId() throws StarRocksException {
        LivyResource resource = new LivyResource(resourceName);
        new Expectations(resource) {
            {
                resource.getLivyUrl();
                result = "http://livy:8998";
                minTimes = 0;
            }
        };

        LivySparkSubmitter submitter = new LivySparkSubmitter();
        // should not throw, just log warning and return
        submitter.kill("invalid_id", loadJobId, resource);
    }

    @Test
    public void testSubmitWithWrongResourceType() {
        SparkResource yarnResource = new SparkResource("yarn_resource");
        EtlJobConfig etlJobConfig = new EtlJobConfig(Maps.newHashMap(), etlOutputPath, label, null);
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        SparkLoadAppHandle handle = new SparkLoadAppHandle();
        SparkSubmitParam param = new SparkSubmitParam(loadJobId, label, etlJobConfig,
                yarnResource, brokerDesc, handle, 300);

        LivySparkSubmitter submitter = new LivySparkSubmitter();
        Assertions.assertThrows(IllegalArgumentException.class, () -> submitter.submit(param));
    }

    @Test
    public void testGetStatusWithWrongResourceType() {
        SparkResource yarnResource = new SparkResource("yarn_resource");
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());

        LivySparkSubmitter submitter = new LivySparkSubmitter();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> submitter.getStatus("42", loadJobId, etlOutputPath, yarnResource, brokerDesc));
    }

    @Test
    public void testKillWithWrongResourceType() {
        SparkResource yarnResource = new SparkResource("yarn_resource");

        LivySparkSubmitter submitter = new LivySparkSubmitter();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> submitter.kill("42", loadJobId, yarnResource));
    }
}
