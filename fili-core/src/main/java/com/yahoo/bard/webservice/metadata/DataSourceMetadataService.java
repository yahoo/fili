// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import com.yahoo.bard.webservice.table.PhysicalTable;

import org.joda.time.DateTime;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import javax.inject.Singleton;

/**
 * Defines a wrapper class around the container that holds the segment metadata of all the physical tables.
 */
@Singleton
public class DataSourceMetadataService {

    /**
     * The container that holds the segment metadata for every table. It should support concurrent access.
     */
    private final Map<PhysicalTable, AtomicReference<ConcurrentSkipListMap<DateTime, Map<String, SegmentInfo>>>>
            allSegments;

    /**
     * The collector that accumulates partitions of a segment.
     */
    private static final Collector<SegmentInfo, ?, Map<String, SegmentInfo>> COLLECTOR = partitionsToMapCollector();

    /**
     * Creates a service to store segment metadata.
     */
    public DataSourceMetadataService() {
        this.allSegments = new ConcurrentHashMap<>();
    }

    /**
     * Update the information with respect to the segment metadata of a particular physical table.
     * This operation should be atomic per table.
     *
     * @param table  The physical table to which the metadata are referring to.
     * @param metadata  The updated datasource metadata.
     */
    public void update(PhysicalTable table, DataSourceMetadata metadata) {
        // Group all the segments by the starting date of their interval.
        // Accumulate all the partitions of a segment in a map indexed by their identifier.
        ConcurrentSkipListMap<DateTime, Map<String, SegmentInfo>> current = metadata.getSegments().stream()
                .collect(
                        Collectors.groupingBy(
                                seg -> seg.getInterval().getStart(),
                                ConcurrentSkipListMap::new,
                                Collectors.mapping(SegmentInfo::new, COLLECTOR)
                        )
                );

        allSegments.computeIfAbsent(table, ignored -> new AtomicReference<>()).set(current);
    }

    /**
     * A collector to aggregate all partitions of a segment into a map.
     *
     * @return The collector
     */
    private static Collector<SegmentInfo, ?, Map<String, SegmentInfo>> partitionsToMapCollector() {
        // Supplier: a linked hash map.
        // Accumulator: adds a segment info to the result map keyed by its identifier
        // Combiner: combine two partial result maps
        return Collector.of(
                LinkedHashMap::new,
                (container, segment) -> container.put(segment.getIdentifier(), segment),
                (left, right) -> {
                    left.putAll(right);
                    return left;
                }
        );
    }

    /**
     * Get all the segments associated with the given Set of physical tables.
     *
     * @param physicalTables  A Set of physicalTables used by the DruidQuery
     *
     * @return A set of segments associated with the given tables
     */
    public Set<SortedMap<DateTime, Map<String, SegmentInfo>>> getTableSegments(Set<PhysicalTable> physicalTables) {
        return physicalTables.stream()
                .map(allSegments::get)
                .filter(Objects::nonNull)
                .map(AtomicReference::get)
                .collect(Collectors.toSet());
    }
}
