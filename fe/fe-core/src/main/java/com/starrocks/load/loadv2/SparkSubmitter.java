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
import com.starrocks.common.LoadException;
import com.starrocks.common.StarRocksException;
import com.starrocks.load.EtlStatus;
import com.starrocks.sql.ast.BrokerDesc;

/**
 * Abstracts the submission, status query, and termination of Spark ETL jobs.
 * Different implementations correspond to different submission modes
 * (spark-submit on YARN, Livy REST API, etc.).
 */
public interface SparkSubmitter {

    /**
     * Submit a Spark ETL job.
     *
     * @param param submission parameters
     * @return result containing appId (YARN application ID or Livy batch ID) and handle
     */
    SparkSubmitResult submit(SparkSubmitParam param) throws LoadException;

    /**
     * Query ETL job status.
     *
     * @param appId       YARN application ID or Livy batch ID
     * @param loadJobId   StarRocks load job ID
     * @param etlOutputPath ETL output path on HDFS
     * @param resource    Spark resource configuration
     * @param brokerDesc  broker descriptor for HDFS access
     * @return ETL status
     */
    EtlStatus getStatus(String appId, long loadJobId, String etlOutputPath,
                         SparkResource resource, BrokerDesc brokerDesc) throws StarRocksException;

    /**
     * Kill an ETL job.
     *
     * @param appId     YARN application ID or Livy batch ID
     * @param loadJobId StarRocks load job ID
     * @param resource  Spark resource configuration
     */
    void kill(String appId, long loadJobId, SparkResource resource) throws StarRocksException;
}
