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

    private final boolean isEmpty;
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

            // Store dimensions in pivoted map
            intervalColumns.getValue().get("dimensions").forEach(column ->
                tempDimensionIntervals.computeIfAbsent(column, k -> new LinkedHashSet<>()).add(interval)
            );

            // Store metrics in pivoted map
            intervalColumns.getValue().get("metrics").forEach(column ->
                tempMetricIntervals.computeIfAbsent(column, k -> new LinkedHashSet<>()).add(interval)
            );
        }

        // Stitch the intervals together
        for (Map.Entry<String, Set<Interval>> entry : tempDimensionIntervals.entrySet()) {
            tempDimensionIntervals.put(entry.getKey(), DateTimeUtils.mergeIntervalSet(entry.getValue()));
        }
        for (Map.Entry<String, Set<Interval>> entry: tempMetricIntervals.entrySet()) {
            tempMetricIntervals.put(entry.getKey(), DateTimeUtils.mergeIntervalSet(entry.getValue()));
        }

        dimensionIntervals = Utils.makeImmutable(tempDimensionIntervals);
        metricIntervals = Utils.makeImmutable(tempMetricIntervals);
        isEmpty = dimensionIntervals.isEmpty() && metricIntervals.isEmpty();
    }

    /**
     * A convenience constructor for test code which doesn't pivot raw segment metadata.
     *
     * @param dimensionIntervals  The map of dimension intervals
     * @param metricIntervals  The map of metric intervals
     */
    @SuppressWarnings("unused") // Used by tests only
    private SegmentMetadata(Map<String, Set<Interval>> dimensionIntervals, Map<String, Set<Interval>> metricIntervals) {
        this.dimensionIntervals = Utils.makeImmutable(dimensionIntervals);
        this.metricIntervals = Utils.makeImmutable(metricIntervals);
        isEmpty = dimensionIntervals.isEmpty() && metricIntervals.isEmpty();
    }

    public Map<String, Set<Interval>> getDimensionIntervals() {
        return dimensionIntervals;
    }

    public Map<String, Set<Interval>> getMetricIntervals() {
        return metricIntervals;
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    @Override
    public boolean equals(Object o) {
        if (o != null && o instanceof SegmentMetadata) {
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
