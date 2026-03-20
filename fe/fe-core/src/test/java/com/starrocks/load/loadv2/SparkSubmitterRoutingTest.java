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
import com.starrocks.common.FeConstants;
import com.starrocks.load.loadv2.etl.EtlJobConfig;
import com.starrocks.sql.ast.BrokerDesc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SparkSubmitterRoutingTest {

    @BeforeEach
    public void setUp() {
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testCreateYarnSubmitter() {
        SparkResource resource = new SparkResource("spark_yarn");
        // no livyUrl set, should route to YarnSparkSubmitter
        Assertions.assertFalse(resource.isLivyMode());

        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        SparkSubmitter submitter = handler.createSubmitter(resource);
        Assertions.assertInstanceOf(YarnSparkSubmitter.class, submitter);
    }

    @Test
    public void testCreateLivySubmitter() {
        LivyResource resource = new LivyResource("spark_livy");
        Assertions.assertTrue(resource.isLivyMode());

        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        SparkSubmitter submitter = handler.createSubmitter(resource);
        Assertions.assertInstanceOf(LivySparkSubmitter.class, submitter);
    }

    @Test
    public void testSparkSubmitParam() {
        SparkResource resource = new SparkResource("spark0");
        EtlJobConfig etlJobConfig = new EtlJobConfig(Maps.newHashMap(), "/output", "label", null);
        BrokerDesc brokerDesc = new BrokerDesc("broker0", Maps.newHashMap());
        SparkLoadAppHandle handle = new SparkLoadAppHandle();

        SparkSubmitParam param = new SparkSubmitParam(
                1L, "label", etlJobConfig, resource, brokerDesc, handle, 300);

        Assertions.assertEquals(1L, param.getLoadJobId());
        Assertions.assertEquals("label", param.getLoadLabel());
        Assertions.assertSame(etlJobConfig, param.getEtlJobConfig());
        Assertions.assertSame(resource, param.getResource());
        Assertions.assertSame(brokerDesc, param.getBrokerDesc());
        Assertions.assertSame(handle, param.getHandle());
        Assertions.assertEquals(300, param.getSubmitTimeoutMs());
    }

    @Test
    public void testSparkSubmitResult() {
        SparkLoadAppHandle handle = new SparkLoadAppHandle();
        SparkSubmitResult result = new SparkSubmitResult("app_123", handle);

        Assertions.assertEquals("app_123", result.getAppId());
        Assertions.assertSame(handle, result.getHandle());
    }
}
