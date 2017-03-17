// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DRUID_METADATA_SEGMENTS_MISSING;

import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.util.DefaultingDictionary;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

/**
 * An implementation of the QuerySigningService that generates segment id for requested interval.
 * It uses the sum of segment hashes to create a segment id.
 */
public class SegmentIntervalsHashIdGenerator implements QuerySigningService<Long> {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentIntervalsHashIdGenerator.class);

    /**
     * The dictionary of the physical tables.
     */
    private final PhysicalTableDictionary physicalTableDictionary;

    /**
     * signingFunctions maps a Class to the Function that should be used to compute requestedIntervals for
     * a given query.
     */
    private final Map<Class, RequestedIntervalsFunction> signingFunctions;

    /**
     * A Service to get information about segment metadata.
     */
    private final DataSourceMetadataService dataSourceMetadataService;

    /**
     * Build a SegmentIntervalsHashIdGenerator that generates a segmentId using the provided signingFunctions.
     *
     * @param physicalTableDictionary  The dictionary of the physical tables
     * @param dataSourceMetadataService  A Service to get information about segment metadata
     * @param signingFunctions  signingFunctions maps a Class to the Function that should be used to compute
     * requestedIntervals for a given query
     */
    public SegmentIntervalsHashIdGenerator(
            PhysicalTableDictionary physicalTableDictionary,
            DataSourceMetadataService dataSourceMetadataService,
            Map<Class, RequestedIntervalsFunction> signingFunctions
    ) {
        this.physicalTableDictionary = physicalTableDictionary;
        this.dataSourceMetadataService = dataSourceMetadataService;
        this.signingFunctions = signingFunctions;
    }

    /**
     * Build a SegmentIntervalsHashIdGenerator that uses the raw simplified intervals of a druidAggregationQuery to
     * create a segmentId.
     *
     * @param physicalTableDictionary  The dictionary of the physical tables
     * @param dataSourceMetadataService  A Service to get information about segment metadata
     */
    public SegmentIntervalsHashIdGenerator(
            PhysicalTableDictionary physicalTableDictionary,
            DataSourceMetadataService dataSourceMetadataService
    ) {
        this(
                physicalTableDictionary,
                dataSourceMetadataService,
                new DefaultingDictionary<>(
                        druidAggregationQuery -> new SimplifiedIntervalList(druidAggregationQuery.getIntervals())
                )
        );
    }

    @Override
    public Optional<Long> getSegmentSetId(DruidAggregationQuery<?> query) {
        //get physical tables
        Set<String> tableNames = query.getInnermostQuery().getDataSource().getNames();
        Set<PhysicalTable> physicalTables = tableNames.stream()
                .map(physicalTableDictionary::get)
                .collect(Collectors.toSet());

        //get all table segments
        Set<SortedMap<DateTime, Map<String, SegmentInfo>>> tableSegments =
                dataSourceMetadataService.getTableSegments(
                        physicalTables.stream()
                                .map(PhysicalTable::getTableName)
                                .collect(Collectors.toSet())
                );

        // Check if we have no tables with segments
        if (tableSegments.isEmpty()) {
            LOG.warn(DRUID_METADATA_SEGMENTS_MISSING.logFormat(tableNames));
            return Optional.empty();
        }

        //get requested intervals
        SimplifiedIntervalList requestedIntervals = signingFunctions.get(query.getClass()).apply(query);

        //get requested segments
        Set<SortedMap<DateTime, Map<String, SegmentInfo>>> requestedSegments = requestedIntervals.stream()
                .flatMap(interval -> tableSegments.stream()
                                .map(segments -> segments.subMap(interval.getStart(), interval.getEnd()))
                )
                .collect(Collectors.toSet());
        //return segment hash
        return Optional.of(getSegmentHash(requestedSegments));
    }

    /**
     * Given a set of requested segments, calculate a hash to represent the segment set Id.
     *
     * @param requestedSegments  A set of requestedSegments
     *
     * @return A hash of the given segments
     */
    public long getSegmentHash(Set<SortedMap<DateTime, Map<String, SegmentInfo>>> requestedSegments) {
        return requestedSegments.stream()
                .mapToLong(Object::hashCode)
                .sum();
    }
}
