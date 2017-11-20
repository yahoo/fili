// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import static com.yahoo.bard.webservice.data.Columns.DIMENSIONS
import static com.yahoo.bard.webservice.data.Columns.METRICS

import com.yahoo.bard.webservice.data.Columns
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import com.google.common.collect.Range
import com.google.common.collect.RangeSet
import com.google.common.collect.TreeRangeSet

import org.joda.time.DateTime
import org.joda.time.Interval

import io.druid.timeline.DataSegment

class DataSourceMetadataSpec extends BaseDataSourceMetadataSpec {
    DataSourceMetadata metadata
    List<DataSegment> allSegments
    Interval interval12

    @Override
    def childSetupSpec() {
        tableName = generateTableName()
        intervals = generateIntervals()
        dimensions = generateDimensions()
        metrics = generateMetrics()
        segments = generateSegments()
    }

    def setup() {
        allSegments = segments.values().toList()
        metadata = new DataSourceMetadata(tableName, [:], allSegments)
        interval12 = intervals.interval12
    }

    def "test construct healthy datasource metadata (interval version)"() {
        setup:
        Map<Columns, Map<String, SimplifiedIntervalList>> intervalLists = DataSourceMetadata.getIntervalLists(metadata)

        expect:
        metadata.getName() == tableName
        metadata.getProperties() == [:]
        metadata.getSegments() == allSegments
        intervalLists.size() == 2
        intervalLists.get(DIMENSIONS).keySet().sort() == dimensions.keySet().sort()
        intervalLists.get(DIMENSIONS).values() as List == [
                [interval12] as SimplifiedIntervalList,
                [interval12] as SimplifiedIntervalList,
                [interval12] as SimplifiedIntervalList
        ]
        intervalLists.get(METRICS).keySet().sort() == metrics.keySet().sort()
        intervalLists.get(METRICS).values() as List == [
                [interval12] as SimplifiedIntervalList,
                [interval12] as SimplifiedIntervalList,
                [interval12] as SimplifiedIntervalList
        ]
    }

    def "test construct healthy datasource metadata (rangeSet version)"() {
        setup:
        Map<Columns, Map<String, RangeSet<DateTime>>> rangeLists = DataSourceMetadata.getRangeLists(metadata)

        RangeSet<DateTime> rangeSet = TreeRangeSet.create()
        rangeSet.add(Range.closedOpen(interval12.getStart(), interval12.getEnd()))

        expect:
        metadata.getName() == tableName
        metadata.getProperties() == [:]
        metadata.getSegments() == allSegments
        rangeLists.size() == 2
        rangeLists.get(DIMENSIONS).keySet().sort() == dimensions.keySet().sort()
        rangeLists.get(DIMENSIONS).values() as List == [rangeSet, rangeSet, rangeSet]
        rangeLists.get(METRICS).keySet().sort() == metrics.keySet().sort()
        rangeLists.get(METRICS).values() as List == [rangeSet, rangeSet, rangeSet]
    }
}
