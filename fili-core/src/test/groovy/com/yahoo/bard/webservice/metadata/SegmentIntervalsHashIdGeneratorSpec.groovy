// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.datasource.QueryDataSource
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.LookbackQuery
import com.yahoo.bard.webservice.druid.model.query.LookbackQuerySpec
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuerySpec
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.util.DefaultingDictionary
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.DateTime
import org.joda.time.Period

import spock.lang.Shared
import spock.lang.Unroll

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Function

import static org.joda.time.DateTimeZone.UTC

class SegmentIntervalsHashIdGeneratorSpec extends BaseDataSourceMetadataSpec {
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

    def setupSpec() {
        segmentInfoMap1 = new LinkedHashMap<>()
        segmentInfoMap2 = new LinkedHashMap<>()
        segmentInfoMap3 = new LinkedHashMap<>()

        segmentInfoMap1.put(segment1.getIdentifier(), new SegmentInfo(segment1))
        segmentInfoMap1.put(segment2.getIdentifier(), new SegmentInfo(segment2))

        segmentInfoMap2.put(segment3.getIdentifier(), new SegmentInfo(segment3))

        jtb = new JerseyTestBinder()
        tableDict = jtb.configurationLoader.getPhysicalTableDictionary()
        metadataService = jtb.testBinderFactory.getDataSourceMetaDataService()

        segmentSetIdGenerator = new SegmentIntervalsHashIdGenerator(
                tableDict,
                metadataService
        )

        Map<Class, RequestedIntervalsFunction> signingFunctions = new DefaultingDictionary<>({new SimplifiedIntervalList(it.getIntervals())} as RequestedIntervalsFunction)
        signingFunctions.put(LookbackQuery.class, new LookbackQuery.LookbackQueryRequestedIntervalsFunction())

        customSegmentSetIdGenerator = new SegmentIntervalsHashIdGenerator(
                tableDict,
                metadataService,
                signingFunctions
        )

        availabilityList1 = new ConcurrentSkipListMap<>()

        availabilityList1.put(interval1.getStart(), segmentInfoMap1)
        availabilityList1.put(interval2.getStart(), segmentInfoMap2)

        availabilityList2 = new ConcurrentSkipListMap<>()
        availabilityList2.put(interval2.getStart(), segmentInfoMap2)

        AtomicReference<ConcurrentSkipListMap<DateTime, Map<String, SegmentInfo>>> atomicRef = new AtomicReference<>()
        atomicRef.set(availabilityList1)

        metadataService.allSegments.put(
                tableDict.get(tableName),
                atomicRef
        );

        TimeSeriesQuerySpec timeSeriesQuerySpec = new TimeSeriesQuerySpec()
        timeSeriesQuery = timeSeriesQuerySpec.defaultQuery(
                intervals: [interval2],
                dataSource: new TableDataSource(new PhysicalTable(tableName, DefaultTimeGrain.DAY.buildZonedTimeGrain(UTC), [:]))
        )

        LookbackQuerySpec lookbackQuerySpec = new LookbackQuerySpec()
        lookbackQuery = lookbackQuerySpec.defaultQuery(
                dataSource: new QueryDataSource(timeSeriesQuery),
                lookbackOffsets: [Period.days(-1)]
        )
    }

    def cleanupSpec() {
        jtb.tearDown()
    }

    def "test metadata service returns valid segment ids"() {
        setup:
        DataSource<?> dataSource = Mock(DataSource)
        dataSource.getNames() >> ([tableName] as Set)
        DruidAggregationQuery<?> query = Mock(DruidAggregationQuery)
        query.getIntervals() >> [interval1, interval2]
        query.getInnermostQuery() >> query
        query.getDataSource() >> dataSource

        when:
        Optional<Long> hashCode = segmentSetIdGenerator.getSegmentSetId(query)

        then:
        hashCode.isPresent() && hashCode.get() == availabilityList1.hashCode() as Long
    }

    @Unroll
    def "test getSegmentHash produces the #expectedHash for #requestedSegment"() {
        expect:
        segmentSetIdGenerator.getSegmentHash(requestedSegment) == expectedHash

        where:
        requestedSegment                                | expectedHash
        [] as Set                                       | 0 as Long
        [availabilityList1] as Set                      | availabilityList1.hashCode()
        [availabilityList2, availabilityList1] as Set   | (availabilityList2.hashCode() + availabilityList1.hashCode()) as long
    }

    def "test different segments have different hashcodes"() {
        expect:
        segmentSetIdGenerator.getSegmentHash([availabilityList1] as Set) != segmentSetIdGenerator.getSegmentHash([availabilityList2] as Set)
    }


    @Unroll
    def "test SegmentIntervalsHashGenerator with custom QuerySigningService returns valid #segmentId for a given #query"() {
        when:
        Optional<Long> hashCode = customSegmentSetIdGenerator.getSegmentSetId(query)

        then:
        hashCode.isPresent() && hashCode.get() == segmentId

        where:
        query           | segmentId
        timeSeriesQuery | availabilityList2.hashCode() as Long
        lookbackQuery   | availabilityList1.hashCode() as Long
    }
}
