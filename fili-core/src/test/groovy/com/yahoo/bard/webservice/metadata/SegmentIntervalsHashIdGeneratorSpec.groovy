// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.datasource.QueryDataSource
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.LookbackQuery
import com.yahoo.bard.webservice.druid.model.query.LookbackQuerySpec
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuerySpec
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.table.TableTestUtils
import com.yahoo.bard.webservice.util.DefaultingDictionary
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.DateTime
import org.joda.time.Interval
import org.joda.time.Period

import spock.lang.Shared
import spock.lang.Unroll

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicReference

class SegmentIntervalsHashIdGeneratorSpec extends BaseDataSourceMetadataSpec {
    @Shared
    Interval interval1
    @Shared
    Interval interval2
    @Shared
    Map<String, SegmentInfo> segmentInfoMap1
    @Shared
    Map<String, SegmentInfo> segmentInfoMap2
    @Shared
    Map<String, SegmentInfo> segmentInfoMap3
    @Shared
    SegmentIntervalsHashIdGenerator segmentSetIdGenerator
    @Shared
    SegmentIntervalsHashIdGenerator customSegmentSetIdGenerator
    @Shared
    JerseyTestBinder jtb
    @Shared
    PhysicalTableDictionary tableDict
    @Shared
    DataSourceMetadataService metadataService
    @Shared
    ConcurrentSkipListMap<DateTime, Map<String, SegmentInfo>> availabilityList1
    @Shared
    ConcurrentSkipListMap<DateTime, Map<String, SegmentInfo>> availabilityList2
    @Shared
    TimeSeriesQuery timeSeriesQuery
    @Shared
    LookbackQuery lookbackQuery

    @Override
    def childSetupSpec() {
        tableName = generateTableName()
        intervals = generateIntervals()
        segments = generateSegments()
    }

    def setupSpec() {
        interval1 = intervals.interval1
        interval2 = intervals.interval2

        segmentInfoMap1 = [:] as LinkedHashMap
        segmentInfoMap2 = [:] as LinkedHashMap
        segmentInfoMap3 = [:] as LinkedHashMap

        segmentInfoMap1[segments.segment1.identifier] = new SegmentInfo(segments.segment1)
        segmentInfoMap1[segments.segment2.identifier] = new SegmentInfo(segments.segment2)

        segmentInfoMap2[segments.segment3.identifier] = new SegmentInfo(segments.segment3)

        jtb = new JerseyTestBinder()
        tableDict = jtb.configurationLoader.physicalTableDictionary
        metadataService = new DataSourceMetadataService()

        segmentSetIdGenerator = new SegmentIntervalsHashIdGenerator(metadataService)

        Map<Class, RequestedIntervalsFunction> signingFunctions = new DefaultingDictionary<>(
                { DruidAggregationQuery query -> new SimplifiedIntervalList(query.intervals) } as RequestedIntervalsFunction
        )
        signingFunctions.put(LookbackQuery, new LookbackQuery.LookbackQueryRequestedIntervalsFunction())

        customSegmentSetIdGenerator = new SegmentIntervalsHashIdGenerator(metadataService, signingFunctions)

        availabilityList1 = [
                (interval1.start): segmentInfoMap1,
                (interval2.start): segmentInfoMap2
        ] as ConcurrentSkipListMap

        availabilityList2 = [(interval2.start): segmentInfoMap2] as ConcurrentSkipListMap

        AtomicReference<ConcurrentSkipListMap<DateTime, Map<String, SegmentInfo>>> atomicRef = new AtomicReference<>()
        atomicRef.set(availabilityList1)

        metadataService.allSegmentsByTime.put(
                tableDict.get(tableName).dataSourceNames[0],
                atomicRef
        )

        timeSeriesQuery = new TimeSeriesQuerySpec().defaultQuery(
                intervals: [interval2],
                dataSource: new TableDataSource(
                        TableTestUtils.buildTable(
                                tableName,
                                DefaultTimeGrain.DAY.buildZonedTimeGrain(UTC),
                                [] as Set,
                                [:],
                                Mock(DataSourceMetadataService) {getAvailableIntervalsByDataSource(_ as DataSourceName) >> [:]}
                        )
                )
        )

        lookbackQuery = new LookbackQuerySpec().defaultQuery(
                dataSource: new QueryDataSource(timeSeriesQuery),
                lookbackOffsets: [Period.days(-1)]
        )
    }

    def cleanupSpec() {
        jtb.tearDown()
    }

    def "test metadata service returns valid segment ids"() {
        setup:
        DataSource dataSource = Mock(DataSource)
        dataSource.physicalTable >> Mock(ConstrainedTable) {
            getDataSourceNames() >> ([DataSourceName.of(tableName)] as Set)
        }

        DruidAggregationQuery<?> query = Mock(DruidAggregationQuery)
        query.intervals >> [interval1, interval2]
        query.innermostQuery >> query
        query.dataSource >> dataSource

        when:
        Optional<Long> hashCode = segmentSetIdGenerator.getSegmentSetId(query)

        then:
        hashCode.present && hashCode.get() == availabilityList1.hashCode() as Long
    }

    @Unroll
    def "test getSegmentHash produces the #expectedHash for #requestedSegment"() {
        expect:
        segmentSetIdGenerator.getSegmentHash(requestedSegment.stream()) == expectedHash

        where:
        requestedSegment                                | expectedValue
        [] as Set                                       | null
        [availabilityList1] as Set                      | availabilityList1.hashCode()
        [availabilityList2, availabilityList1] as Set   | availabilityList2.hashCode() + availabilityList1.hashCode()

        expectedHash = !expectedValue ? Optional.empty() : Optional.of(expectedValue as long)
    }

    def "test different segments have different hashcodes"() {
        expect:
        segmentSetIdGenerator.getSegmentHash([availabilityList1].stream()).get() != segmentSetIdGenerator.getSegmentHash([availabilityList2].stream()).get()
    }

    @Unroll
    def "test SegmentIntervalsHashGenerator with custom QuerySigningService returns valid #segmentId for a given #query"() {
        when:
        Optional<Long> hashCode = customSegmentSetIdGenerator.getSegmentSetId(query)

        then:
        hashCode.present && hashCode.get() == segmentId

        where:
        query           | segmentId
        timeSeriesQuery | availabilityList2.hashCode() as Long
        lookbackQuery   | availabilityList1.hashCode() as Long
    }
}
