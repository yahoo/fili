// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import com.yahoo.bard.webservice.util.DateTimeUtils;
import com.yahoo.bard.webservice.util.Utils;

import org.joda.time.Interval;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The segment metadata from a particular physical table/druid data source.
 */
public class SegmentMetadata {

    private final Map<String, Set<Interval>> dimensionIntervals;
    private final Map<String, Set<Interval>> metricIntervals;

    /**
     * Create a map of dimension and metric intervals from a map of intervals to dimensions and metrics.
     *
     * @param queryResult  The java mapped data objects built directly from the Segment Metadata endpoint JSON
     */
    public SegmentMetadata(Map<String, Map<String, List<String>>> queryResult) {
        Map<String, Set<Interval>> tempDimensionIntervals = new HashMap<>();
        Map<String, Set<Interval>> tempMetricIntervals = new HashMap<>();

        for (Map.Entry<String, Map<String, List<String>>> intervalColumns : queryResult.entrySet()) {
            Interval interval = Interval.parse(intervalColumns.getKey());
            List<String> dimensions = intervalColumns.getValue().get("dimensions");
            List<String> metrics = intervalColumns.getValue().get("metrics");

            // Store dimensions in pivoted map
            for (String dimensionColumn: dimensions) {
                if (! tempDimensionIntervals.containsKey(dimensionColumn)) {
                    tempDimensionIntervals.put(dimensionColumn, new LinkedHashSet<>());
                }
                // Add the new interval into the set
                Set<Interval> intervals = tempDimensionIntervals.get(dimensionColumn);
                intervals.add(interval);
            }

            // Store metrics in pivoted map
            for (String metricColumn : metrics) {
                if (! tempMetricIntervals.containsKey(metricColumn)) {
                    tempMetricIntervals.put(metricColumn, new LinkedHashSet<>());
                }

                // Add the new interval into the set
                Set<Interval> intervals = tempMetricIntervals.get(metricColumn);
                intervals.add(interval);
            }
        }

        // Stitch the intervals together
        for (String columnKey : tempDimensionIntervals.keySet()) {
            Set<Interval> mergedSet = DateTimeUtils.mergeIntervalSet(tempDimensionIntervals.get(columnKey));
            tempDimensionIntervals.put(columnKey, mergedSet);
        }
        for (String columnKey : tempMetricIntervals.keySet()) {
            Set<Interval> mergedSet = DateTimeUtils.mergeIntervalSet(tempMetricIntervals.get(columnKey));
            tempMetricIntervals.put(columnKey, mergedSet);
        }

        dimensionIntervals = Utils.makeImmutable(tempDimensionIntervals);
        metricIntervals = Utils.makeImmutable(tempMetricIntervals);
    }

    /**
     * A convenience constructor for test code which doesn't pivot raw segment metadata.
     *
     * @param dimensionIntervals  The map of dimension intervals
     * @param metricIntervals  The map of metric intervals
     */
    @SuppressWarnings("unused")
    private SegmentMetadata(Map<String, Set<Interval>> dimensionIntervals, Map<String, Set<Interval>> metricIntervals) {
        this.dimensionIntervals = Utils.makeImmutable(dimensionIntervals);
        this.metricIntervals = Utils.makeImmutable(metricIntervals);
    }

    public Map<String, Set<Interval>> getDimensionIntervals() {
        return dimensionIntervals;
    }

    public Map<String, Set<Interval>> getMetricIntervals() {
        return metricIntervals;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SegmentMetadata) {
            SegmentMetadata that = (SegmentMetadata) o;
            return this.dimensionIntervals.equals(that.dimensionIntervals) &&
                this.metricIntervals.equals(that.metricIntervals);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dimensionIntervals, metricIntervals);
    }
}
