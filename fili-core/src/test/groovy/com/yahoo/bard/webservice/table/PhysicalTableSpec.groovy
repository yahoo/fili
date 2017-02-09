// Copyright 2017 Yahoo Inc.
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
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class PhysicalTableSpec extends Specification {

    @Shared ConcretePhysicalTable physicalTable
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
        dimension = new KeyValueStoreDimension("dimension", null, [BardDimensionField.ID] as LinkedHashSet, MapStoreManager.getInstance("dimension"), ScanSearchProviderManager.getInstance("apiProduct"))

        physicalTable = new ConcretePhysicalTable(
                "test table",
                DAY.buildZonedTimeGrain(UTC),
                [new DimensionColumn(dimension)] as Set,
                ['dimension': 'druidDim']
        )
        dimensionDictionary = new DimensionDictionary([dimension].toSet())

        dimensionColumn = new DimensionColumn(dimension)

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
        physicalTable.getIntervalsByColumnName(column).toList() == new SimplifiedIntervalList(expected) as List

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
        table = new ConcretePhysicalTable(
                name,
                YEAR.buildZonedTimeGrain(UTC),
                [new DimensionColumn(dimension)] as Set,
                ["dimension": "druidDim"]
        )

        then:
        table.getAvailability() != null
        table.getAvailability().getAvailableIntervals().isEmpty()

        when:
        table.resetColumns(noMetricMetadata, dimensionDictionary)

        then:
        table.availableIntervals.containsKey(dimensionColumn)
        table.availableIntervals.get(dimensionColumn).toList() == new SimplifiedIntervalList(intervalSet3) as List
        table.getDimensions() == [dimension] as Set
        table.getSchema().getColumns(MetricColumn.class) == [] as Set

        when:
        table.resetColumns(segmentMetadata, dimensionDictionary)

        then:
        table.getDimensions() == [dimension] as Set
        table.getIntervalsByColumnName(metricColumn1.name).toList() == new SimplifiedIntervalList(intervalSet2) as List
    }

    def "test the setColumnCache() method"() {
        expect:
        physicalTable.getAvailableIntervals() == cache.collectEntries {[(it.key) : (new SimplifiedIntervalList(it.value).toList())]}
    }

    def "test the getIntervalsByColumnName() method"() {
        expect:
        physicalTable.getIntervalsByColumnName("metric2").toList() == new SimplifiedIntervalList(intervalSet3).toList()
    }

    def "test the fetching of all dimensions from the table"() {
        expect:
        physicalTable.getDimensions() == [dimension] as Set
    }

    def "test physical to logical mapping is constructed correctly"() {
        setup:
        PhysicalTable oneDimPhysicalTable = new ConcretePhysicalTable(
                "test table", DAY.buildZonedTimeGrain(UTC),
                [new DimensionColumn(dimension)] as Set,
                ['dimension': 'druidDim']
        )
        PhysicalTable twoDimPhysicalTable = new ConcretePhysicalTable(
                "test table", DAY.buildZonedTimeGrain(UTC),
                [new DimensionColumn(dimension)] as Set,
                ['dimension1': 'druidDim', 'dimension2': 'druidDim']
        )

        expect:
        oneDimPhysicalTable.getSchema().getLogicalColumnNames('druidDim') == ['dimension'] as Set
        twoDimPhysicalTable.getSchema().getLogicalColumnNames('druidDim') == ['dimension1', 'dimension2'] as Set
    }
}
