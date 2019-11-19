// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName

import org.joda.time.DateTime
import org.joda.time.Interval

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Collectors

class DataSourceMetadataServiceSpec extends BaseDataSourceMetadataSpec {

    DataSourceMetadata metadata

    @Override
    def childSetupSpec() {
        tableName = generateTableName()
        intervals = generateIntervals()
        dimensions = generateDimensions()
        metrics = generateMetrics()
        segments = generateSegments()
    }

    def setup() {
        metadata = new DataSourceMetadata(tableName, [:], segments.values().toList())
    }

    def "test metadata service updates segment availability for physical tables and access methods behave correctly"() {
        setup:
        JerseyTestBinder jtb = new JerseyTestBinder()
        DataSourceName dataSourceName = DataSourceName.of(tableName)

        DataSourceMetadataService metadataService = new DataSourceMetadataService()

        when:
        metadataService.update(dataSourceName, metadata)

        then:
        metadataService.allSegmentsByTime.get(dataSourceName) instanceof AtomicReference
        metadataService.allSegmentsByColumn.get(dataSourceName) instanceof AtomicReference

        and:
        metadataService.getSegments([dataSourceName] as Set).stream()
                .map({it.values()})
                .flatMap({it.stream()})
                .map({it.values()})
                .map({it.collect {it.identifier}})
                .collect(Collectors.toList())  == [
                        [segments.segment1.identifier, segments.segment2.identifier],
                        [segments.segment3.identifier, segments.segment4.identifier]
                ]

        and: "all the intervals by column in metadata service are simplified to interval12"
        [[intervals["interval12"]]].containsAll(metadataService.getAvailableIntervalsByDataSource(dataSourceName).values())

        cleanup:
        jtb.tearDown()
    }

    def "grouping segment data by date time behave as expected"() {
        given:
        ConcurrentSkipListMap<DateTime, Map<String, SegmentInfo>> segmentByTime = DataSourceMetadataService
                .groupSegmentByTime(metadata)
        DateTime dateTime1 = new DateTime(intervals["interval1"].start)
        DateTime dateTime2 = new DateTime(intervals["interval2"].start)

        expect:
        segmentByTime.keySet() == [dateTime1, dateTime2] as Set
        segmentByTime.get(new DateTime(intervals["interval2"].start)).keySet() == [segments.segment3.identifier, segments.segment4.identifier] as Set
    }

    def "grouping intervals by column behave as expected"() {
        given:
        Map<String, List<Interval>> intervalByColumn = DataSourceMetadataService.groupIntervalByColumn(metadata)

        expect:
        intervalByColumn.keySet() == dimensions.keySet() + metrics.keySet()
        intervalByColumn.get(dimensions.(TestApiDimensionName.BREED.asName()).asName()) == [intervals["interval12"]]
    }

    def "accessing availability by column throws exception if the table does not exist in datasource metadata service"() {
        setup:
        DataSourceMetadataService metadataService = new DataSourceMetadataService()

        when:
        metadataService.getAvailableIntervalsByDataSource(DataSourceName.of("InvalidTable"))

        then:
        IllegalStateException e = thrown()
        e.message == "Datasource 'InvalidTable' is not available in the metadata service"
    }
}
