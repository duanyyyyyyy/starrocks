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
    public void testSubmit(@Mocked BrokerUtil brokerUtil) throws LoadException {
        LivyBatchResponse mockResponse = new LivyBatchResponse();
        mockResponse.setId(42);
        mockResponse.setState("starting");

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
                result = livyUrl;
                resource.getWorkingDir();
                result = "hdfs://127.0.0.1:10000/tmp/starrocks";
            }
        };

        EtlJobConfig etlJobConfig = new EtlJobConfig(Maps.newHashMap(), etlOutputPath, label, null);
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        SparkLoadAppHandle handle = new SparkLoadAppHandle();
        SparkSubmitParam param = new SparkSubmitParam(loadJobId, label, etlJobConfig,
                resource, brokerDesc, handle, Config.spark_load_submit_timeout_second);

        LivySparkSubmitter submitter = new LivySparkSubmitter();
        SparkSubmitResult result = submitter.submit(param);
        Assertions.assertEquals("42", result.getAppId());
    }

    @Test
    public void testGetStatus() throws StarRocksException {
        LivyBatchResponse mockResponse = new LivyBatchResponse();
        mockResponse.setId(42);
        mockResponse.setState("running");
        mockResponse.setAppId("application_123_001");
        mockResponse.setAppInfo(Map.of("sparkUiUrl", "http://spark-ui:4040"));

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
    public void testGetStatusInvalidBatchId() throws StarRocksException {
        LivyResource resource = new LivyResource(resourceName);
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        LivySparkSubmitter submitter = new LivySparkSubmitter();
        EtlStatus status = submitter.getStatus("not_a_number", loadJobId, etlOutputPath, resource, brokerDesc);
        Assertions.assertEquals(TEtlState.CANCELLED, status.getState());
    }

    @Test
    public void testKill() throws StarRocksException {
        new MockUp<LivyClient>() {
            @Mock
            public void deleteBatch(int batchId) {
            }
        };

        LivyResource resource = new LivyResource(resourceName);
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
        LivyResource resource = new LivyResource(resourceName);
        LivySparkSubmitter submitter = new LivySparkSubmitter();
        submitter.kill("invalid", loadJobId, resource);
    }
}
