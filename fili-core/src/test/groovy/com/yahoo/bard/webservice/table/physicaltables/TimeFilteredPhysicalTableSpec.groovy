// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.physicaltables

import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.table.ConfigPhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableSchema
import com.yahoo.bard.webservice.table.availability.Availability
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class TimeFilteredPhysicalTableSpec extends Specification {

    @Shared TimeFilteredPhysicalTable timeFilteredPhysicalTable
    @Shared ConfigPhysicalTable basePhysicalTable
    @Shared DimensionDictionary dimensionDictionary

    @Shared Dimension dimension
    @Shared DimensionColumn dimensionColumn
    @Shared MetricColumn metricColumn1, metricColumn2

    @Shared SimplifiedIntervalList filterInterval
    @Shared Set<Interval> intervalSet1, intervalSet2, intervalSet3

    @Shared Map<String, Set<Interval>> segmentMetadata
    @Shared Map<String, Set<Interval>> logicalColumnToInterval

    TableName tableName
    Availability mockAvailability

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
        intervalSet3 = [interval3] as Set

        segmentMetadata = [
                'dimension_one' : (intervalSet1),
                'metric_one'    : (intervalSet2),
                'metric_two'    : (intervalSet3)
        ]

        logicalColumnToInterval = [
                'dimensionOne' : (intervalSet1),
                'metricOne'    : (intervalSet2),
                'metricTwo'    : (intervalSet3)
        ]

        filterInterval = new SimplifiedIntervalList(Collections.singletonList(new Interval("2014-07-01/2014-07-03")))
    }

    def setup() {
        basePhysicalTable = Mock(ConfigPhysicalTable)
        mockAvailability = Mock(Availability)
        basePhysicalTable.getAvailability() >> mockAvailability

        tableName = Mock(TableName)
        tableName.asName() >> 'test table'

        timeFilteredPhysicalTable = new TimeFilteredPhysicalTable(
                tableName,
                basePhysicalTable,
                { filterInterval }
        )
    }

    @Unroll
    def "getAvailableIntervals with constraint intersects the underlying table's getAvailableIntervals(constraint) with it's own time filter, returning #expected for column #column"() {
        setup:
        DataSourceConstraint constraints = Mock(DataSourceConstraint)
        constraints.allColumnNames >> [column.name]

        basePhysicalTable.getAvailableIntervals(constraints) >> {
            logicalColumnToInterval[column.name]
        }

        expect:
        timeFilteredPhysicalTable.getAvailableIntervals(constraints) as List == new SimplifiedIntervalList(expected) as List

        where:
        column          | expected
        dimensionColumn | intervalSet1 //time filter overlaps interval1
        metricColumn1   | intervalSet1 //time filter overlaps interval1, but not interval2, so only interval1 should be in result
        metricColumn2   | [] //interval3 does not overlap time filter, so empty result
    }

    def "Physical table getAvailableIntervals WITHOUT constraint intersects the the underlying table's getAvailableIntervals() with it's own time filter"() {
        setup:
        basePhysicalTable.getAvailableIntervals() >> {
            new SimplifiedIntervalList(intervalSet2).union(new SimplifiedIntervalList(intervalSet3))
        }

        expect:
        timeFilteredPhysicalTable.getAvailableIntervals() as List == new SimplifiedIntervalList(intervalSet1)
    }

    def "Creating constrained table throws exception when requesting a column not on the table"() {
        given:
        PhysicalTableSchema schema = Mock(PhysicalTableSchema)
        schema.getColumnNames() >> {
            ['test_columns'] as List
        }
        basePhysicalTable.getSchema() >> schema

        DataSourceConstraint constraints = Mock(DataSourceConstraint)
        constraints.allColumnNames >> ['un_configured']

        when:
        timeFilteredPhysicalTable.withConstraint(constraints)

        then:
        RuntimeException exception = thrown()
        exception.message == 'Received invalid request requesting for columns: un_configured that is not available in this table: test table'
    }

    @Unroll
    def "Creating constrained table then querying the table still filters by time, returning #expected for column #column"() {
        given:
        DataSourceConstraint constraints = Mock(DataSourceConstraint)
        constraints.allColumnNames >> [column.name]

        mockAvailability.getAvailableIntervals(Mock(PhysicalDataSourceConstraint)) >> {
            new SimplifiedIntervalList(logicalColumnToInterval[column.name])
        }
        mockAvailability.getAllAvailableIntervals() >> {
            [
                    (dimensionColumn.name) : new SimplifiedIntervalList(intervalSet1),
                    (metricColumn1.name) : new SimplifiedIntervalList(intervalSet2),
                    (metricColumn2.name) : new SimplifiedIntervalList(intervalSet3)
            ]
        }
        mockAvailability.getDataSourceNames(Mock(PhysicalDataSourceConstraint)) >> {
            [Mock(DataSourceName)] as Set
        }

        PhysicalTableSchema schema = Mock(PhysicalTableSchema)
        schema.getColumnNames() >> { [column.name] as List }
        schema.getColumns() >> { [column] as List }
        basePhysicalTable.getSchema() >> schema

        def constrainedTable = timeFilteredPhysicalTable.withConstraint(constraints)

        expect:
        constrainedTable.getAvailableIntervals() as List == new SimplifiedIntervalList(expected) as List

        where:
        column          | expected
        dimensionColumn | intervalSet1 //time filter overlaps interval1
        metricColumn1   | intervalSet1 //time filter overlaps interval1, but not interval2, so only interval1 should be in result
        metricColumn2   | [] //interval3 does not overlap time filter, so empty result
    }
}
