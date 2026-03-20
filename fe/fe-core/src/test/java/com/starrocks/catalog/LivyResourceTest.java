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
    private String workingDir;
    private String broker;
    private Map<String, String> properties;

    @BeforeEach
    public void setUp() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
        name = "spark_livy0";
        workingDir = "hdfs://127.0.0.1/tmp/starrocks";
        broker = "broker0";
        properties = Maps.newHashMap();
        properties.put("type", "spark_livy");
        properties.put("livy.url", "http://livy-server:8998");
        properties.put("working_dir", workingDir);
        properties.put("broker", broker);
    }

    @Test
    public void testFromStmt(@Injectable BrokerMgr brokerMgr, @Mocked GlobalStateMgr globalStateMgr)
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
        Assertions.assertEquals(name, resource.getName());
        Assertions.assertEquals(Resource.ResourceType.SPARK_LIVY, resource.getType());
        Assertions.assertTrue(resource.isLivyMode());
        Assertions.assertFalse(resource.isYarnMaster());
        Assertions.assertEquals("http://livy-server:8998", resource.getLivyUrl());
        Assertions.assertEquals(workingDir, resource.getWorkingDir());
        Assertions.assertEquals(broker, resource.getBroker());

        // with basic auth
        properties.put("livy.username", "admin");
        properties.put("livy.password", "secret");
        stmt = new CreateResourceStmt(true, name, properties);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
        resource = (LivyResource) Resource.fromStmt(stmt);
        Assertions.assertEquals("admin", resource.getLivyUsername());
        Assertions.assertEquals("secret", resource.getLivyPassword());

        // getCopiedResource preserves all fields
        SparkResource copied = resource.getCopiedResource();
        Assertions.assertInstanceOf(LivyResource.class, copied);
        LivyResource copiedLivy = (LivyResource) copied;
        Assertions.assertEquals("http://livy-server:8998", copiedLivy.getLivyUrl());
        Assertions.assertEquals("admin", copiedLivy.getLivyUsername());

        // getProcNodeData includes livy.url
        BaseProcResult result = new BaseProcResult();
        resource.getProcNodeData(result);
        boolean hasLivyUrl = result.getRows().stream()
                .anyMatch(row -> row.get(2).equals("livy.url"));
        Assertions.assertTrue(hasLivyUrl);
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
            CreateResourceStmt stmt = new CreateResourceStmt(true, name, props);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
            Resource.fromStmt(stmt);
        });
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

        LivyResource updated = (LivyResource) copied;
        Assertions.assertEquals("newuser", updated.getLivyUsername());
        Assertions.assertEquals("newpass", updated.getLivyPassword());
    }

    @Test
    public void testNoBroker(@Injectable BrokerMgr brokerMgr, @Mocked GlobalStateMgr globalStateMgr) {
        assertThrows(DdlException.class, () -> {
            new Expectations() {
                {
                    globalStateMgr.getBrokerMgr();
                    result = brokerMgr;
                    brokerMgr.containsBroker(broker);
                    result = false;
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
            Resource.fromStmt(stmt);
        });
    }
}
