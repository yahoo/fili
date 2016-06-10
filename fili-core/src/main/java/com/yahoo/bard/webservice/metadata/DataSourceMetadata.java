// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import static com.yahoo.bard.webservice.data.Columns.DIMENSIONS;
import static com.yahoo.bard.webservice.data.Columns.METRICS;

import com.yahoo.bard.webservice.data.Columns;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import io.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The segment metadata from a particular physical table/druid data source.
 */
public class DataSourceMetadata {
    private final String name;
    private final Map<String, String> properties;
    private final List<DataSegment> segments;

    /**
     * Store the full metadata for a druid data source, mainly as a list of segments
     *
     * @param name  The name of the druid datasource.
     * @param properties  The map of properties of this datasource.
     * @param segments  The list of segments of this datasource.
     */
    @JsonCreator
    public DataSourceMetadata(
            @JsonProperty("name") String name,
            @JsonProperty("properties") Map<String, String> properties,
            @JsonProperty("segments") List<DataSegment> segments
    ) {
        this.name = name;
        this.properties = Collections.unmodifiableMap(properties);
        this.segments = Collections.unmodifiableList(segments);
    }

    /**
     * Get the name of this datasource.
     *
     * @return The name of the datasource.
     */
    public String getName() {
        return name;
    }

    /**
     * Get the properties of the datasource.
     *
     * @return The map of the properties of the datasource.
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Get the segments of the datasource.
     *
     * @return The list of the segments of the datasource.
     */
    public List<DataSegment> getSegments() {
        return segments;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof DataSourceMetadata)) { return false; }

        DataSourceMetadata that = (DataSourceMetadata) o;

        if (name != null ? !name.equals(that.name) : that.name != null) { return false; }
        if (properties != null ? !properties.equals(that.properties) : that.properties != null) { return false; }
        return !(segments != null ? !segments.equals(that.segments) : that.segments != null);

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        result = 31 * result + (segments != null ? segments.hashCode() : 0);
        return result;
    }

    /**
     * Return a pair of maps containing the simplified intervals for dimensions and metrics respectively.
     *
     * @param metadata  The metadata that contain segment availability.
     *
     * @return A pair of two maps of simplified intervals. One entry is for dimensions and the other for metrics.
     */
    public static Map<Columns, Map<String, SimplifiedIntervalList>> getIntervalLists(DataSourceMetadata metadata) {

        Map<String, Set<Interval>> unsortedDimensionIntervals = new HashMap<>();
        Map<String, Set<Interval>> unsortedMetricIntervals = new HashMap<>();

        metadata.segments.stream()
                .forEach(
                        segment -> {
                            buildIntervalSet(
                                    segment.getDimensions(),
                                    segment.getInterval(),
                                    unsortedDimensionIntervals
                            );
                            buildIntervalSet(segment.getMetrics(), segment.getInterval(), unsortedMetricIntervals);
                        }
                );


        EnumMap<Columns, Map<String, SimplifiedIntervalList>> intervals = new EnumMap<>(Columns.class);
        intervals.put(DIMENSIONS, toSimplifiedIntervalMap(unsortedDimensionIntervals));
        intervals.put(METRICS, toSimplifiedIntervalMap(unsortedMetricIntervals));

        return intervals;
    }

    private static void buildIntervalSet(
            List<String> entries,
            Interval interval,
            Map<String, Set<Interval>> container
    ) {
        entries.stream()
                .map(entry -> container.computeIfAbsent(entry, ignored -> new LinkedHashSet<>()))
                .forEach(set -> set.add(interval));
    }

    private static Map<String, SimplifiedIntervalList> toSimplifiedIntervalMap(
            Map<String, Set<Interval>> unsortedIntervals
    ) {
        return unsortedIntervals.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> new SimplifiedIntervalList(entry.getValue())
                        )
                );
    }

    /**
     * Return a pair of maps containing the ranges of availability for dimensions and metrics respectively.
     *
     * @param metadata  The metadata that contain segment availability.
     *
     * @return A pair of two maps of range sets. One entry is for dimensions and the other for metrics.
     */
    public static Map<Columns, Map<String, RangeSet<DateTime>>> getRangeLists(DataSourceMetadata metadata) {

        final Map<String, RangeSet<DateTime>> dimensionIntervals = new HashMap<>();
        final Map<String, RangeSet<DateTime>> metricIntervals = new HashMap<>();

        metadata.segments.stream()
                .forEach(
                        segment -> {
                            buildRangeSet(segment.getDimensions(), segment.getInterval(), dimensionIntervals);
                            buildRangeSet(segment.getMetrics(), segment.getInterval(), metricIntervals);
                        }
                );

        EnumMap<Columns, Map<String, RangeSet<DateTime>>> intervals = new EnumMap<>(Columns.class);
        intervals.put(DIMENSIONS, dimensionIntervals);
        intervals.put(METRICS, metricIntervals);

        return intervals;
    }

    private static void buildRangeSet(
            List<String> entries,
            Interval interval,
            Map<String, RangeSet<DateTime>> container
    ) {
        entries.stream()
                .map(entry -> container.computeIfAbsent(entry, ignored -> TreeRangeSet.create()))
                .forEach(set -> set.add(Range.closedOpen(interval.getStart(), interval.getEnd())));
    }
}
