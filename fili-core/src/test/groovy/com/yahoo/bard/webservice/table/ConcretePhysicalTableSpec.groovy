// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService
import com.yahoo.bard.webservice.table.availability.ConcreteAvailability
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class ConcretePhysicalTableSpec extends Specification {

    @Shared ConcretePhysicalTable physicalTable
    @Shared DimensionDictionary dimensionDictionary

    @Shared Dimension dimension
    @Shared DimensionColumn dimensionColumn
    @Shared MetricColumn metricColumn1, metricColumn2

    @Shared Set<Interval> intervalSet1, intervalSet2, intervalSet3

    @Shared Map<String, Set<Interval>> segmentMetadata

    def setupSpec() {
        dimension = new KeyValueStoreDimension("dimensionOne", null, [BardDimensionField.ID] as LinkedHashSet, MapStoreManager.getInstance("dimension"), ScanSearchProviderManager.getInstance("apiProduct"))

        dimensionColumn = new DimensionColumn(dimension)
        metricColumn1 = new MetricColumn("metricOne")
        metricColumn2 = new MetricColumn("metricTwo")

        dimensionDictionary = new DimensionDictionary([dimension].toSet())

        Interval interval1 = new Interval("2014-07-01/2014-07-03")
        Interval interval2 = new Interval("2014-07-03/2014-07-05")
        Interval interval3 = new Interval("2014-07-05/2014-07-09")

        intervalSet1 = [interval1] as Set
        intervalSet2 = [interval1, interval2] as Set
        intervalSet3 = [interval1, interval2, interval3] as Set

        segmentMetadata = [
                'dimension_one' : (intervalSet1),
                'metric_one'    : (intervalSet2),
                'metric_two'    : (intervalSet3)
        ]

        physicalTable = new ConcretePhysicalTable(
                TableName.of("test table"),
                DAY.buildZonedTimeGrain(UTC),
                [dimensionColumn, metricColumn1, metricColumn2] as Set,
                ['dimensionOne': 'dimension_one', 'metricOne': 'metric_one', 'metricTwo': 'metric_two'],
                new TestDataSourceMetadataService(segmentMetadata)
        )
    }

    @Unroll
    def "Physical table getAvailableIntervals returns #expected for column #column"() {
        setup:
        DataSourceConstraint constraints = Mock(DataSourceConstraint)
        constraints.getAllColumnNames() >> [column.name]

        expect:
        physicalTable.getAvailableIntervals(constraints) as List == new SimplifiedIntervalList(expected) as List

        where:
        column                      | expected
        dimensionColumn             | intervalSet1
        metricColumn1               | intervalSet2
        metricColumn2               | intervalSet3
        new Column("MissingName")   | [] as Set
    }

    def "test datasource metadata service correctly initializes"() {
        setup:
        String name = "test a"
        PhysicalTable table
        Map<String, Set<Interval>> noMetricMetadata = ['dimension_one' : (intervalSet3)]
        DataSourceConstraint constraints = Mock(DataSourceConstraint)
        constraints.getAllColumnNames() >> [metricColumn1.name]

        when:
        table = new ConcretePhysicalTable(
                TableName.of(name),
                YEAR.buildZonedTimeGrain(UTC),
                [dimensionColumn, metricColumn1] as Set,
                ['dimensionOne': 'dimension_one', 'metricOne': 'metric_one'],
                new TestDataSourceMetadataService(noMetricMetadata)
        )

        then:
        table.getAllAvailableIntervals().containsKey(dimensionColumn)
        table.getAllAvailableIntervals().get(dimensionColumn) as List == new SimplifiedIntervalList(intervalSet3) as List
        table.getDimensions() == [dimension] as Set

        when:
        table.setAvailability(new ConcreteAvailability(physicalTable.getTableName(), new TestDataSourceMetadataService(segmentMetadata)))

        then:
        table.getDimensions() == [dimension] as Set
        table.getAvailableIntervals(constraints) as List == new SimplifiedIntervalList(intervalSet2) as List
    }

    def "test the getIntervalsByColumnName() method"() {
        setup:
        DataSourceConstraint constraints = Mock(DataSourceConstraint)
        constraints.getAllColumnNames() >> [metricColumn2.name]

        expect:
        physicalTable.getAvailableIntervals(constraints).asList() == new SimplifiedIntervalList(intervalSet3).toList()
    }

    def "test the fetching of all dimensions from the table"() {
        expect:
        physicalTable.getDimensions() == [dimension] as Set
    }

    def "test physical to logical mapping is constructed correctly with multiple logical name to one physical name"() {
        setup:
        PhysicalTable oneDimPhysicalTable = new ConcretePhysicalTable(
                TableName.of("test table"),
                DAY.buildZonedTimeGrain(UTC),
                [dimensionColumn] as Set,
                ['dimensionOne': 'dimension_one'],
                Mock(DataSourceMetadataService)
        )
        PhysicalTable twoDimPhysicalTable = new ConcretePhysicalTable(
                TableName.of("test table"),
                DAY.buildZonedTimeGrain(UTC),
                [dimensionColumn] as Set,
                ['dimensionOne': 'dimension_one', 'dimensionTwo': 'dimension_one'],
                Mock(DataSourceMetadataService)
        )

        expect:
        oneDimPhysicalTable.getSchema().getLogicalColumnNames('dimension_one') == ['dimensionOne'] as Set
        twoDimPhysicalTable.getSchema().getLogicalColumnNames('dimension_one') == ['dimensionOne', 'dimensionTwo'] as Set
    }
}
