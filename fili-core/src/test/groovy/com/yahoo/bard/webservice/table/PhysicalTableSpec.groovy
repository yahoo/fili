// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.metadata.SegmentMetadata

import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class PhysicalTableSpec extends Specification {

    @Shared PhysicalTable physicalTable
    @Shared DimensionDictionary dimensionDictionary

    @Shared DimensionColumn dimensionColumn
    @Shared MetricColumn metricColumn1
    @Shared MetricColumn metricColumn2

    @Shared Set<Interval> intervalSet1
    @Shared Set<Interval> intervalSet2
    @Shared Set<Interval> intervalSet3

    @Shared Map cache
    @Shared SegmentMetadata segmentMetadata
    @Shared Dimension dimension

    def setupSpec() {
        physicalTable = new PhysicalTable("test table", DAY.buildZonedTimeGrain(UTC), ['dimension':'druidDim'])
        dimension = new KeyValueStoreDimension("dimension", null, [BardDimensionField.ID] as LinkedHashSet, MapStoreManager.getInstance("dimension"), ScanSearchProviderManager.getInstance("apiProduct"))
        dimensionDictionary = new DimensionDictionary([dimension] as Set)

        dimensionColumn = new DimensionColumn(dimension, physicalTable.getPhysicalColumnName(dimension.getApiName()))

        metricColumn1 = new MetricColumn("metric1")
        metricColumn2 = new MetricColumn("metric2")

        Interval interval1 = new Interval("2014-07-01/2014-07-03")
        Interval interval2 = new Interval("2014-07-03/2014-07-05")
        Interval interval3 = new Interval("2014-07-05/2014-07-09")

        intervalSet1 = [interval1] as Set
        intervalSet2 = [interval1, interval2] as Set
        intervalSet3 = [interval1, interval2, interval3] as Set

        segmentMetadata = new SegmentMetadata(
            [(dimensionColumn.name): (intervalSet1)],
            [
                (metricColumn1.name): (intervalSet2),
                (metricColumn2.name): (intervalSet3)
            ]
            )

        cache =[
                (dimensionColumn): (intervalSet1),
                (metricColumn1): (intervalSet2),
                (metricColumn2): (intervalSet3)
        ]
    }

    def setup() {
        physicalTable.resetColumns(segmentMetadata, dimensionDictionary)
    }

    @Unroll
    def "Physical table getColumnAvailability returns #expected for column #column"() {
        expect:
        physicalTable.getIntervalsByColumnName(column) == expected

        where:
        column               | expected
        dimensionColumn.name | intervalSet1
        metricColumn1.name   | intervalSet2
        metricColumn2.name   | intervalSet3
        "MissingName"        | [] as Set
    }

    def "test reset columns correctly initializes"() {
        setup:
        String name = "test a"
        PhysicalTable table
        SegmentMetadata noMetricMetadata = new SegmentMetadata(
            [(dimensionColumn.name): (intervalSet3)],
            [:]
            )
        when:
        table = new PhysicalTable(name, YEAR.buildZonedTimeGrain(UTC), ["dimension":"druidDim"])

        then:
        table.availableIntervalsRef.get() != null
        table.availableIntervals.isEmpty()
        table.workingIntervals.isEmpty()

        when:
        table.resetColumns(noMetricMetadata, dimensionDictionary)

        then:
        table.availableIntervals.containsKey(dimensionColumn)
        table.availableIntervals.get(dimensionColumn) == intervalSet3
        table.getDimensions() == [dimension] as Set
        table.getColumns(MetricColumn.class) == [] as Set
        table.workingIntervals.isEmpty()

        when:
        table.resetColumns(segmentMetadata, dimensionDictionary)

        then:
        table.getDimensions() == [dimension] as Set
        table.getIntervalsByColumnName(metricColumn1.name) == intervalSet2
    }

    def "test the addition of columns to the table cache"() {
        setup:
        physicalTable.addColumn(dimensionColumn)
        physicalTable.addColumn(metricColumn1)
        physicalTable.addColumn(metricColumn2)
        physicalTable.commit()

        def expectedCache = [(dimensionColumn): [] as Set, (metricColumn1): [] as Set, (metricColumn2) :[] as Set]

        expect:
        physicalTable.getAvailableIntervals() == expectedCache
    }

    def "test the setColumnCache() method"() {
        expect:
        physicalTable.getAvailableIntervals() == cache
    }

    def "test the getIntervalsByColumnName() method"() {
        expect:
        physicalTable.getIntervalsByColumnName("metric2") == intervalSet3
    }

    def "test the fetching of all dimensions from the table"() {
        expect:
        physicalTable.getDimensions() == [dimension] as Set
    }

    def "test physical to logical mapping is constructed correctly"() {
        setup:
        PhysicalTable oneDimPhysicalTable = new PhysicalTable("test table", DAY.buildZonedTimeGrain(UTC), ['dimension':'druidDim'])
        PhysicalTable twoDimPhysicalTable = new PhysicalTable("test table", DAY.buildZonedTimeGrain(UTC), ['dimension1':'druidDim', 'dimension2':'druidDim'])

        expect:
        oneDimPhysicalTable.getLogicalColumnNames('druidDim') == ['dimension'] as Set
        twoDimPhysicalTable.getLogicalColumnNames('druidDim') == ['dimension1', 'dimension2'] as Set
    }
}
