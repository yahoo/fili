// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import com.google.common.collect.ImmutableMap;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.druid.timeline.DataSegment;

import java.util.HashSet;
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
    private static final Logger LOG = LoggerFactory.getLogger(DataSourceMetadataService.class);

    /**
     * The container that holds the segment metadata for every table. It should support concurrent access.
     */
    private final Map<TableName, AtomicReference<ConcurrentSkipListMap<DateTime, Map<String, SegmentInfo>>>>
            allSegmentsByTime;
    private final Map<TableName, AtomicReference<ImmutableMap<String, Set<Interval>>>>
            allSegmentsByColumn;

    /**
     * The collector that accumulates partitions of a segment.
     */
    private static final Collector<SegmentInfo, ?, Map<String, SegmentInfo>> COLLECTOR = partitionsToMapCollector();

    /**
     * Creates a service to store segment metadata.
     */
    public DataSourceMetadataService() {
        this.allSegmentsByTime = new ConcurrentHashMap<>();
        this.allSegmentsByColumn = new ConcurrentHashMap<>();
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
        ConcurrentSkipListMap<DateTime, Map<String, SegmentInfo>> currentByTime = metadata.getSegments().stream()
                .collect(
                        Collectors.groupingBy(
                                seg -> seg.getInterval().getStart(),
                                ConcurrentSkipListMap::new,
                                Collectors.mapping(SegmentInfo::new, COLLECTOR)
                        )
                );

        // Group segment interval by every column present in the segment
        Map<String, Set<Interval>> currentByColumn = new LinkedHashMap<>();

        // Accumulate all intervals by column name
        for (DataSegment segment : metadata.getSegments()) {
            SegmentInfo segmentInfo = new SegmentInfo(segment);
            for (String column : segmentInfo.getColumnNames()) {
                currentByColumn.computeIfAbsent(column, ignored -> new HashSet<>()).add(segmentInfo.getInterval());
            }
        }

        // Simplify interval sets using SimplifiedIntervalList
        Map<String, Set<Interval>> simplifiedByColumn = currentByColumn.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> new HashSet<>(new SimplifiedIntervalList(entry.getValue()))
                        )
                );

        allSegmentsByTime.computeIfAbsent(table.getTableName(), ignored -> new AtomicReference<>())
                .set(currentByTime);
        allSegmentsByColumn.computeIfAbsent(table.getTableName(), ignored -> new AtomicReference<>())
                .set(ImmutableMap.copyOf(simplifiedByColumn));
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
     * @param physicalTableNames  A Set of physical TableNames used by the DruidQuery
     *
     * @return A set of segments associated with the given tables
     */
    public Set<SortedMap<DateTime, Map<String, SegmentInfo>>> getTableSegments(Set<TableName> physicalTableNames) {
        return physicalTableNames.stream()
                .map(allSegmentsByTime::get)
                .filter(Objects::nonNull)
                .map(AtomicReference::get)
                .collect(Collectors.toSet());
    }

    /**
     * Get a set of intervals available for each column in the table.
     *
     * @param physicalTableName  The table to get the column and availability for
     *
     * @return a map of column name to a set of avialable intervals
     */
    public Map<String, Set<Interval>> getAvailableIntervalsByTable(TableName physicalTableName) {
        if (!allSegmentsByColumn.containsKey(physicalTableName)) {
            LOG.error(
                    "Trying to access {} physical table datasource that is not available in metadata service",
                    physicalTableName.asName()
            );
            throw new IllegalStateException(
                    String.format(
                            "Trying to access %s physical table datasource that is not available in metadata service",
                            physicalTableName.asName()
                    )
            );
        }
        return allSegmentsByColumn.get(physicalTableName).get();
    }
}
