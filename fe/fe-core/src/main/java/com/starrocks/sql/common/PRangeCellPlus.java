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

package com.starrocks.sql.common;

import com.google.common.collect.Range;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.PartitionKey;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * {@link PRangeCellPlus} contains a {@link PRangeCell} and its partition's name to
 * represent a table's partition range info.
 */
public class PRangeCellPlus implements Comparable<PRangeCellPlus> {
    private final PRangeCell cell;
    private final String partitionName;

    public PRangeCellPlus(String partitionName, Range<PartitionKey> partitionKeyRange) {
        this.partitionName = partitionName;
        this.cell = new PRangeCell(partitionKeyRange);
    }

    public PRangeCellPlus(String partitionName, PRangeCell rangeCell) {
        this.partitionName = partitionName;
        this.cell = rangeCell;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public PRangeCell getCell() {
        return cell;
    }

    public boolean isIntersected(PRangeCellPlus o) {
        return cell.isIntersected(o.getCell());
    }

    /**
     * Get intersected cells.
     *
     * @param cell PRangeCellPlus whose intersected cells will be found.
     * @param ranges need to be sorted.
     * @return intersected cells
     */
    public static List<PRangeCellPlus> getIntersectedCells(PRangeCellPlus cell, List<PRangeCellPlus> ranges) {
        if (ranges.isEmpty()) {
            return List.of();
        }
        List<PartitionKey> lowerPoints = ranges.stream().map(
                dstRange -> dstRange.getCell().getRange().lowerEndpoint()).toList();
        List<PartitionKey> upperPoints = ranges.stream().map(
                dstRange -> dstRange.getCell().getRange().upperEndpoint()).toList();

        PartitionKey lower = cell.getCell().getRange().lowerEndpoint();
        PartitionKey upper = cell.getCell().getRange().upperEndpoint();

        // For an interval [l, r], if there exists another interval [li, ri] that intersects with it, this interval
        // must satisfy l ≤ ri and r ≥ li. Therefore, if there exists a pos_a such that for all k < pos_a,
        // ri[k] < l, and there exists a pos_b such that for all k > pos_b, li[k] > r, then all intervals between
        // pos_a and pos_b might potentially intersect with the interval [l, r].
        int posA = PartitionKey.findLastLessEqualInOrderedList(lower, upperPoints);
        int posB = PartitionKey.findLastLessEqualInOrderedList(upper, lowerPoints);

        List<PRangeCellPlus> results = new LinkedList<>();
        for (int i = posA; i <= posB; ++i) {
            if (ranges.get(i).isIntersected(cell)) {
                results.add(ranges.get(i));
            }
        }
        return results;
    }

    public static List<PRangeCellPlus> toPRangeCellPlus(Map<String, PCell> rangeMap) {
        return rangeMap.entrySet().stream()
                .map(e -> new PRangeCellPlus(e.getKey(), (PRangeCell) e.getValue()))
                .sorted(PRangeCellPlus::compareTo)
                .collect(Collectors.toList());
    }

    /**
     * Convert range map to list of partition range cell plus which is sorted by range cell.
     * @param rangeMap range map to be converted
     * @param isConvertToDate whether convert to date partition range which is used for base table with string partition column
     * @return sorted list of partition range cell plus
     */
    public static List<PRangeCellPlus> toPRangeCellPlus(Map<String, Range<PartitionKey>> rangeMap,
                                                        boolean isConvertToDate) {
        return rangeMap.entrySet().stream()
                .map(e -> new PRangeCellPlus(e.getKey(),
                        isConvertToDate ? SyncPartitionUtils.convertToDatePartitionRange(e.getValue()) : e.getValue()))
                .sorted(PRangeCellPlus::compareTo)
                .collect(Collectors.toList());
    }

    /**
     * Convert a range map to list of partition range cell plus which is sorted by range cell.
     */
    public static List<PRangeCellPlus> toPRangeCellPlus(Map<String, PCell> rangeMap,
                                                        Expr expr) {
        return rangeMap.entrySet().stream()
                .map(e -> {
                    Range<PartitionKey> partitionKeyRanges = ((PRangeCell) e.getValue()).getRange();
                    Range<PartitionKey> convertRanges = SyncPartitionUtils.convertToDatePartitionRange(partitionKeyRanges);
                    return new PRangeCellPlus(e.getKey(), SyncPartitionUtils.transferRange(convertRanges, expr));
                })
                .sorted(PRangeCellPlus::compareTo)
                .collect(Collectors.toList());
    }

    @Override
    public int compareTo(PRangeCellPlus o) {
        return cell.compareTo(o.getCell());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null || !(o instanceof PRangeCellPlus)) {
            return false;
        }
        PRangeCellPlus range = (PRangeCellPlus) o;
        return this.partitionName.equals(((PRangeCellPlus) o).partitionName) &&
                this.cell.equals(range.cell);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionName, cell);
    }

    @Override
    public String toString() {
        return "PRangeCellPlus{" +
                "name=" + partitionName +
                "cell=" + cell +
                '}';
    }
}
