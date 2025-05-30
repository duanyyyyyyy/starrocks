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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/task/engine_storage_migration_task.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/Types_constants.h"
#include "storage/olap_define.h"
#include "storage/snapshot_manager.h"
#include "storage/tablet_updates.h"
#include "storage/task/engine_task.h"

namespace starrocks {

// base class for storage engine
// add "Engine" as task prefix to prevent duplicate name with agent task
class EngineStorageMigrationTask : public EngineTask {
public:
    Status execute() override;

public:
    EngineStorageMigrationTask(TTabletId tablet_id, TSchemaHash schema_hash, DataDir* dest_store,
                               bool need_rebuild_pk_index);
    ~EngineStorageMigrationTask() override = default;

private:
    Status _storage_migrate(TabletSharedPtr tablet);

    void _generate_new_header(DataDir* store, const uint64_t new_shard, const TabletSharedPtr& tablet,
                              const std::vector<RowsetSharedPtr>& consistent_rowsets,
                              const TabletMetaSharedPtr& new_tablet_meta);

    Status _copy_index_and_data_files(const std::string& header_path, const TabletSharedPtr& ref_tablet,
                                      const std::vector<RowsetSharedPtr>& consistent_rowsets) const;

    Status _finish_migration(const TabletSharedPtr& tablet, int64_t end_version, uint64_t shard,
                             const std::vector<RowsetSharedPtr>& consistent_rowsets,
                             const std::shared_ptr<TabletMeta>& new_tablet_meta, const string& schema_hash_path,
                             std::string& new_meta_file, std::string& dcgs_snapshot_path, bool& need_remove_new_path);

    Status _finish_primary_key_migration(const TabletSharedPtr& tablet, int64_t end_version, uint64_t shard,
                                         const std::vector<RowsetSharedPtr>& consistent_rowsets,
                                         const string& schema_hash_path, std::string& new_meta_file,
                                         bool& need_remove_new_path);

private:
    TTabletId _tablet_id;
    TSchemaHash _schema_hash;
    DataDir* _dest_store;
    bool _need_rebuild_pk_index{false};
}; // EngineTask
} // namespace starrocks
