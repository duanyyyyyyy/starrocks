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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.MultiOpPattern;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarRangePredicateExtractor;
import com.starrocks.sql.optimizer.rewrite.TimeDriftConstraint;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PushDownPredicateScanRule extends TransformationRule {
    private static final ImmutableSet<OperatorType> SUPPORT = ImmutableSet.of(
            OperatorType.LOGICAL_OLAP_SCAN,
            OperatorType.LOGICAL_HIVE_SCAN,
            OperatorType.LOGICAL_ICEBERG_SCAN,
            OperatorType.LOGICAL_HUDI_SCAN,
            OperatorType.LOGICAL_DELTALAKE_SCAN,
            OperatorType.LOGICAL_FILE_SCAN,
            OperatorType.LOGICAL_PAIMON_SCAN,
            OperatorType.LOGICAL_ODPS_SCAN,
            OperatorType.LOGICAL_ICEBERG_METADATA_SCAN,
            OperatorType.LOGICAL_ICEBERG_EQUALITY_DELETE_SCAN,
            OperatorType.LOGICAL_KUDU_SCAN,
            OperatorType.LOGICAL_SCHEMA_SCAN,
            OperatorType.LOGICAL_ES_SCAN,
            OperatorType.LOGICAL_META_SCAN,
            OperatorType.LOGICAL_BINLOG_SCAN,
            OperatorType.LOGICAL_VIEW_SCAN,
            OperatorType.LOGICAL_TABLE_FUNCTION_TABLE_SCAN
    );

    public PushDownPredicateScanRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_SCAN, Pattern.create(OperatorType.LOGICAL_FILTER).addChildren(
                MultiOpPattern.of(SUPPORT)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator lfo = (LogicalFilterOperator) input.getOp();

        OptExpression scan = input.getInputs().get(0);
        LogicalScanOperator logicalScanOperator = (LogicalScanOperator) scan.getOp();

        ScalarOperatorRewriter scalarOperatorRewriter = new ScalarOperatorRewriter();
        ScalarOperator predicates = Utils.compoundAnd(lfo.getPredicate(), logicalScanOperator.getPredicate());

        predicates = ScalarOperatorRewriter.simplifyCaseWhen(predicates);

        ScalarRangePredicateExtractor rangeExtractor = new ScalarRangePredicateExtractor();
        predicates = rangeExtractor.rewriteOnlyColumn(Utils.compoundAnd(Utils.extractConjuncts(predicates)
                .stream().map(rangeExtractor::rewriteOnlyColumn).collect(Collectors.toList())));
        Preconditions.checkState(predicates != null);

        predicates = scalarOperatorRewriter.rewrite(predicates,
                ScalarOperatorRewriter.DEFAULT_REWRITE_SCAN_PREDICATE_RULES);
        predicates = Utils.transTrue2Null(predicates);

        predicates = TimeDriftConstraint.tryAddDerivedPredicates(predicates, logicalScanOperator.getTable(),
                logicalScanOperator.getColumnNameToColRefMap());

        // clone a new scan operator and rewrite predicate.
        Operator.Builder builder = OperatorBuilderFactory.build(logicalScanOperator);
        LogicalScanOperator newScanOperator = (LogicalScanOperator) builder.withOperator(logicalScanOperator)
                .setPredicate(predicates)
                .build();
        newScanOperator.buildColumnFilters(predicates);
        Map<ColumnRefOperator, ScalarOperator> projectMap =
                newScanOperator.getOutputColumns().stream()
                        .collect(Collectors.toMap(Function.identity(), Function.identity()));
        LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(projectMap);
        OptExpression project = OptExpression.create(logicalProjectOperator, OptExpression.create(newScanOperator));
        return Lists.newArrayList(project);
    }
}
