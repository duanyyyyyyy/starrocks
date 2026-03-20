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

import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.ResourceDesc;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class LivyResourceTest {
    private static ConnectContext connectContext;
    private String name;
    private String livyUrl;
    private String workingDir;
    private String broker;
    private Map<String, String> properties;

    @BeforeEach
    public void setUp() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
        name = "spark_livy0";
        livyUrl = "http://livy-server:8998";
        workingDir = "hdfs://127.0.0.1/tmp/starrocks";
        broker = "broker0";
        properties = Maps.newHashMap();
        properties.put("type", "spark_livy");
        properties.put("livy.url", livyUrl);
        properties.put("working_dir", workingDir);
        properties.put("broker", broker);
    }

    @Test
    public void testCreateLivyResource(@Injectable BrokerMgr brokerMgr, @Mocked GlobalStateMgr globalStateMgr)
            throws StarRocksException {
        new Expectations() {
            {
                globalStateMgr.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = true;
            }
        };

        Analyzer analyzer = new Analyzer(Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                globalStateMgr.getAnalyzer();
                result = analyzer;
            }
        };

        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        Resource resource = Resource.fromStmt(stmt);

        Assertions.assertInstanceOf(LivyResource.class, resource);
        LivyResource livyResource = (LivyResource) resource;
        Assertions.assertTrue(livyResource.isLivyMode());
        Assertions.assertFalse(livyResource.isYarnMaster());
        Assertions.assertEquals(livyUrl, livyResource.getLivyUrl());
        Assertions.assertEquals(workingDir, livyResource.getWorkingDir());
        Assertions.assertEquals(broker, livyResource.getBroker());
        Assertions.assertEquals(Resource.ResourceType.SPARK_LIVY, livyResource.getType());
    }

    @Test
    public void testLivyBasicAuth(@Injectable BrokerMgr brokerMgr, @Mocked GlobalStateMgr globalStateMgr)
            throws StarRocksException {
        new Expectations() {
            {
                globalStateMgr.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = true;
            }
        };

        Analyzer analyzer = new Analyzer(Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                globalStateMgr.getAnalyzer();
                result = analyzer;
            }
        };

        properties.put("livy.username", "admin");
        properties.put("livy.password", "secret");
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        LivyResource resource = (LivyResource) Resource.fromStmt(stmt);

        Assertions.assertEquals("admin", resource.getLivyUsername());
        Assertions.assertEquals("secret", resource.getLivyPassword());
    }

    @Test
    public void testMissingLivyUrl(@Mocked GlobalStateMgr globalStateMgr) {
        assertThrows(DdlException.class, () -> {
            Analyzer analyzer = new Analyzer(Analyzer.AnalyzerVisitor.getInstance());
            new Expectations() {
                {
                    globalStateMgr.getAnalyzer();
                    result = analyzer;
                }
            };

            Map<String, String> props = Maps.newHashMap();
            props.put("type", "spark_livy");
            // missing livy.url
            CreateResourceStmt stmt = new CreateResourceStmt(true, name, props);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
            Resource.fromStmt(stmt);
        });
    }

    @Test
    public void testGetCopiedResource(@Injectable BrokerMgr brokerMgr, @Mocked GlobalStateMgr globalStateMgr)
            throws StarRocksException {
        new Expectations() {
            {
                globalStateMgr.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = true;
            }
        };

        Analyzer analyzer = new Analyzer(Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                globalStateMgr.getAnalyzer();
                result = analyzer;
            }
        };

        properties.put("livy.username", "admin");
        properties.put("livy.password", "secret");
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        LivyResource resource = (LivyResource) Resource.fromStmt(stmt);

        SparkResource copied = resource.getCopiedResource();
        Assertions.assertInstanceOf(LivyResource.class, copied);
        LivyResource copiedLivy = (LivyResource) copied;
        Assertions.assertTrue(copiedLivy.isLivyMode());
        Assertions.assertEquals(livyUrl, copiedLivy.getLivyUrl());
        Assertions.assertEquals("admin", copiedLivy.getLivyUsername());
        Assertions.assertEquals("secret", copiedLivy.getLivyPassword());
        Assertions.assertEquals(workingDir, copiedLivy.getWorkingDir());
        Assertions.assertEquals(broker, copiedLivy.getBroker());
    }

    @Test
    public void testUpdate(@Injectable BrokerMgr brokerMgr, @Mocked GlobalStateMgr globalStateMgr)
            throws StarRocksException {
        new Expectations() {
            {
                globalStateMgr.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = true;
            }
        };

        Analyzer analyzer = new Analyzer(Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                globalStateMgr.getAnalyzer();
                result = analyzer;
            }
        };

        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        LivyResource resource = (LivyResource) Resource.fromStmt(stmt);

        SparkResource copied = resource.getCopiedResource();
        Map<String, String> updateProps = Maps.newHashMap();
        updateProps.put("livy.username", "newuser");
        updateProps.put("livy.password", "newpass");
        ResourceDesc desc = new ResourceDesc(name, updateProps);
        copied.update(desc);

        LivyResource updatedLivy = (LivyResource) copied;
        Assertions.assertEquals("newuser", updatedLivy.getLivyUsername());
        Assertions.assertEquals("newpass", updatedLivy.getLivyPassword());
    }

    @Test
    public void testGetProcNodeData(@Injectable BrokerMgr brokerMgr, @Mocked GlobalStateMgr globalStateMgr)
            throws StarRocksException {
        new Expectations() {
            {
                globalStateMgr.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = true;
            }
        };

        Analyzer analyzer = new Analyzer(Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                globalStateMgr.getAnalyzer();
                result = analyzer;
            }
        };

        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        LivyResource resource = (LivyResource) Resource.fromStmt(stmt);

        BaseProcResult result = new BaseProcResult();
        resource.getProcNodeData(result);
        boolean hasLivyUrl = result.getRows().stream()
                .anyMatch(row -> row.get(2).equals("livy.url"));
        Assertions.assertTrue(hasLivyUrl);
    }

    @Test
    public void testSparkConfigPassthrough(@Injectable BrokerMgr brokerMgr, @Mocked GlobalStateMgr globalStateMgr)
            throws StarRocksException {
        new Expectations() {
            {
                globalStateMgr.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = true;
            }
        };

        Analyzer analyzer = new Analyzer(Analyzer.AnalyzerVisitor.getInstance());
        new Expectations() {
            {
                globalStateMgr.getAnalyzer();
                result = analyzer;
            }
        };

        // spark.* configs should be captured for pass-through to Livy
        properties.put("spark.executor.memory", "2g");
        properties.put("spark.yarn.queue", "default");
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        LivyResource resource = (LivyResource) Resource.fromStmt(stmt);

        Assertions.assertEquals("2g", resource.getSparkConfigs().get("spark.executor.memory"));
        Assertions.assertEquals("default", resource.getSparkConfigs().get("spark.yarn.queue"));
    }
}
