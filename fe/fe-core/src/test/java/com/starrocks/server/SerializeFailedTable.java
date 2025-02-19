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

package com.starrocks.server;

import com.starrocks.catalog.Table;
import com.starrocks.persist.gson.GsonPreProcessable;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class SerializeFailedTable extends Table implements GsonPreProcessable {

    public SerializeFailedTable(long id, String name) {
        super(id, name, TableType.OLAP, new ArrayList<>());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new IOException("failed");
    }

    @Override
    public void gsonPreProcess() throws IOException {
        throw new IOException("failed");
    }
}
