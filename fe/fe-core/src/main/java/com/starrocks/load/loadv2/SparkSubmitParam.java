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

import com.starrocks.catalog.SparkResource;
import com.starrocks.load.loadv2.etl.EtlJobConfig;
import com.starrocks.sql.ast.BrokerDesc;

/**
 * Immutable parameter object for Spark ETL job submission.
 */
public class SparkSubmitParam {
    private final long loadJobId;
    private final String loadLabel;
    private final EtlJobConfig etlJobConfig;
    private final SparkResource resource;
    private final BrokerDesc brokerDesc;
    private final SparkLoadAppHandle handle;
    private final long submitTimeoutMs;

    public SparkSubmitParam(long loadJobId, String loadLabel, EtlJobConfig etlJobConfig,
                            SparkResource resource, BrokerDesc brokerDesc,
                            SparkLoadAppHandle handle, long submitTimeoutMs) {
        this.loadJobId = loadJobId;
        this.loadLabel = loadLabel;
        this.etlJobConfig = etlJobConfig;
        this.resource = resource;
        this.brokerDesc = brokerDesc;
        this.handle = handle;
        this.submitTimeoutMs = submitTimeoutMs;
    }

    public long getLoadJobId() {
        return loadJobId;
    }

    public String getLoadLabel() {
        return loadLabel;
    }

    public EtlJobConfig getEtlJobConfig() {
        return etlJobConfig;
    }

    public SparkResource getResource() {
        return resource;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public SparkLoadAppHandle getHandle() {
        return handle;
    }

    public long getSubmitTimeoutMs() {
        return submitTimeoutMs;
    }
}
