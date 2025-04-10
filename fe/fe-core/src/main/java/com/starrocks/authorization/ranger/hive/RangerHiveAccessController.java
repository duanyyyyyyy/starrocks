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

package com.starrocks.authorization.ranger.hive;

import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.ranger.RangerAccessController;
import com.starrocks.catalog.Column;
import com.starrocks.qe.ConnectContext;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class RangerHiveAccessController extends RangerAccessController {
    public RangerHiveAccessController(String serviceName) {
        super("hive", serviceName);
    }

    @Override
    public void checkDbAction(ConnectContext context, String catalogName, String db,
                              PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(RangerHiveResource.builder()
                        .setDatabase(db)
                        .build(),
                context.getCurrentUserIdentity(),
                context.getGroups(),
                privilegeType);
    }

    @Override
    public void checkAnyActionOnDb(ConnectContext context, String catalogName, String db)
            throws AccessDeniedException {
        hasPermission(RangerHiveResource.builder()
                        .setDatabase(db)
                        .build(),
                context.getCurrentUserIdentity(),
                context.getGroups(),
                PrivilegeType.ANY);
    }

    @Override
    public void checkTableAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(RangerHiveResource.builder()
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .build(),
                context.getCurrentUserIdentity(),
                context.getGroups(),
                privilegeType);
    }

    @Override
    public void checkAnyActionOnTable(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        hasPermission(RangerHiveResource.builder()
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .build(),
                context.getCurrentUserIdentity(),
                context.getGroups(),
                PrivilegeType.ANY);
    }

    @Override
    public void checkColumnAction(ConnectContext context, TableName tableName,
                                  String column, PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(RangerHiveResource.builder()
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .setColumn(column)
                        .build(),
                context.getCurrentUserIdentity(),
                context.getGroups(),
                privilegeType);
    }

    @Override
    public Map<String, Expr> getColumnMaskingPolicy(ConnectContext context, TableName tableName, List<Column> columns) {
        Map<String, Expr> maskingExprMap = Maps.newHashMap();
        for (Column column : columns) {
            Expr columnMaskingExpression = getColumnMaskingExpression(RangerHiveResource.builder()
                    .setDatabase(tableName.getDb())
                    .setTable(tableName.getTbl())
                    .setColumn(column.getName())
                    .build(), column, context);
            if (columnMaskingExpression != null) {
                maskingExprMap.put(column.getName(), columnMaskingExpression);
            }
        }

        return maskingExprMap;
    }

    @Override
    public Expr getRowAccessPolicy(ConnectContext context, TableName tableName) {
        return getRowAccessExpression(RangerHiveResource.builder()
                .setDatabase(tableName.getDb())
                .setTable(tableName.getTbl())
                .build(), context);
    }

    @Override
    public String convertToAccessType(PrivilegeType privilegeType) {
        HiveAccessType hiveAccessType;
        if (privilegeType == PrivilegeType.SELECT) {
            hiveAccessType = HiveAccessType.SELECT;
        } else if (privilegeType == PrivilegeType.INSERT) {
            hiveAccessType = HiveAccessType.UPDATE;
        } else if (privilegeType == PrivilegeType.CREATE_DATABASE
                || privilegeType == PrivilegeType.CREATE_TABLE
                || privilegeType == PrivilegeType.CREATE_VIEW) {
            hiveAccessType = HiveAccessType.CREATE;
        } else if (privilegeType == PrivilegeType.DROP) {
            hiveAccessType = HiveAccessType.DROP;
        } else {
            hiveAccessType = HiveAccessType.NONE;
        }

        return hiveAccessType.name().toLowerCase(Locale.ENGLISH);
    }
}

