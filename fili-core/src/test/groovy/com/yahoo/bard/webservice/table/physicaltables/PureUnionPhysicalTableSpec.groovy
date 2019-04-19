// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.physicaltables

import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.table.BaseSchema
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.ConfigPhysicalTable
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.table.PhysicalTableSchema
import com.yahoo.bard.webservice.table.availability.Availability
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class PureUnionPhysicalTableSpec extends Specification {

    ConfigPhysicalTable baseTable1, baseTable2
    BaseSchema schema1, schema2
    Column column1, column2, column3
    LinkedHashSet physicalColumns1, physicalColumns2
    Map logicalToPhysicalNames1, logicalToPhysicalNames2
    @Shared String physicalColumnName1, physicalColumnName2, physicalColumnName3, logicalColumnName1, logicalColumnName2_1, logicalColumnName2_2, logicalColumnName3

    def setupSpec() {
        physicalColumnName1 = 'physicalColumn1'
        physicalColumnName2= 'physicalColumn2'
        physicalColumnName3= 'physicalColumn3'

        logicalColumnName1 = 'logicalColumn1'
        logicalColumnName2_1 = 'logicalColumn2_1'
        logicalColumnName2_2 = 'logicalColumn2_2'
        logicalColumnName3 = 'logicalColumn3'
    }

    def setup() {
        column1 = new Column(physicalColumnName1)
        column2 = new Column(physicalColumnName2)
        column3 = new Column(physicalColumnName3)

        physicalColumns1 = [column1, column2] as Set
        physicalColumns2 = [column2, column3] as Set

        logicalToPhysicalNames1 = [
                (logicalColumnName1) : (physicalColumnName1),
                (logicalColumnName2_1) : (physicalColumnName2)
        ]

        logicalToPhysicalNames2 = [
                (logicalColumnName2_2) : (physicalColumnName2),
                (logicalColumnName3) : (physicalColumnName3)
        ]

        ZonedTimeGrain grain = Mock(ZonedTimeGrain)
        schema1 = new PhysicalTableSchema(grain, physicalColumns1, logicalToPhysicalNames1)
        schema2 = new PhysicalTableSchema(grain, physicalColumns2, logicalToPhysicalNames2)

        baseTable1 = Mock(ConfigPhysicalTable)
        baseTable2 = Mock(ConfigPhysicalTable)
        baseTable1.getAvailability() >> Mock(Availability)
        baseTable2.getAvailability() >> Mock(Availability)
        baseTable1.getSchema() >> schema1
        baseTable2.getSchema() >> schema2

        baseTable1
    }

    def "Generate schema unions all columns of sub tables and deduplicates columns" () {
        setup:
        PureUnionPhysicalTable unionPhysicalTable = new PureUnionPhysicalTable(Mock(TableName), [baseTable1, baseTable2] as Set)

        expect:
        unionPhysicalTable.getSchema().getColumns() == [ column1, column2, column3 ] as Set
    }

    @Unroll
    def "Generated schema merges logical to physical column mapping"() {
        setup:
        PureUnionPhysicalTable unionPhysicalTable = new PureUnionPhysicalTable(Mock(TableName), [baseTable1, baseTable2] as Set)

        expect:
        logicalNames.each { unionPhysicalTable.getSchema().getPhysicalColumnName(it) == physicalName }
        unionPhysicalTable.getSchema().getLogicalColumnNames(physicalName) == logicalNames

        where:
        physicalName        |   logicalNames
        physicalColumnName1 |   [logicalColumnName1] as Set
        physicalColumnName2 |   [logicalColumnName2_1, logicalColumnName2_2] as Set
        physicalColumnName3 |   [logicalColumnName3] as Set
    }

    def "If not all physical table schemas share the same time grain throw an error"() {
        setup:
        schema1 = new PhysicalTableSchema(Mock(ZonedTimeGrain), physicalColumns1, logicalToPhysicalNames1)
        schema2 = new PhysicalTableSchema(Mock(ZonedTimeGrain), physicalColumns2, logicalToPhysicalNames2)

        baseTable1 = Mock(ConfigPhysicalTable)
        baseTable2 = Mock(ConfigPhysicalTable)
        baseTable1.getAvailability() >> Mock(Availability)
        baseTable2.getAvailability() >> Mock(Availability)
        baseTable1.getSchema() >> schema1
        baseTable2.getSchema() >> schema2

        when:
        new PureUnionPhysicalTable(Mock(TableName), [baseTable1, baseTable2] as Set)

        then:
        IllegalStateException exception = thrown()
        exception.message == 'Error: unioned table contains physical tables with more than one time grain. Tried to union tables null, null'
    }

    def "getAvailableIntervals with constraint unions the underlying table's getAvailableIntervals(constraint) properly"() {
        setup:
        DataSourceConstraint constraints = Mock(DataSourceConstraint)

        Set intervalSet1 = []
        Set intervalSet2 = []
        Set expectedIntervals = []
        intervals1.each { it -> intervalSet1.add(new Interval(it)) }
        intervals2.each { it -> intervalSet2.add(new Interval(it)) }
        expected.each { it -> expectedIntervals.add(new Interval(it)) }

        baseTable1.getAvailableIntervals(constraints) >> { new SimplifiedIntervalList(intervalSet1) }
        baseTable2.getAvailableIntervals(constraints) >> { new SimplifiedIntervalList(intervalSet2) }

        ConfigPhysicalTable table = new PureUnionPhysicalTable(Mock(TableName), [baseTable1, baseTable2] as Set)

        expect:
        table.getAvailableIntervals(constraints) as List == new SimplifiedIntervalList(expectedIntervals) as List

        where:
        intervals1                                          |   intervals2                                          |   expected
        ['2015-01-01/2015-06-01', '2016-06-01/2017-01-01']  |   ['2015-06-01/2016-01-01', '2016-01-01/2016-06-01']  |   ['2015-01-01/2017-01-01']
        ['2015-01-01/2015-06-01', '2017-01-01/2017-06-01']  |   ['2015-06-01/2016-01-01', '2017-06-01/2018-01-01']  |   ['2015-01-01/2016-01-01', '2017-01-01/2018-01-01']
    }

    def "getAvailableIntervals WITHOUT constraint unions all provided intervals of all sub tables together" () {
        setup:
        baseTable1.getAvailableIntervals() >> {
            new SimplifiedIntervalList([
                    new Interval('2014-01-01/2014-03-01'),
                    new Interval('2015-01-01/2015-03-01'),
                    new Interval('2016-01-01/2016-03-01'),
                    new Interval('2017-01-01/2017-03-01')
            ] as Set)
        }
        baseTable2.getAvailableIntervals() >> {
            new SimplifiedIntervalList([
                    new Interval('2014-03-01/2014-06-01'),
                    new Interval('2015-03-01/2015-06-01'),
                    new Interval('2016-03-01/2016-06-01'),
                    new Interval('2017-03-01/2017-06-01')
            ] as Set)
        }
        ConfigPhysicalTable table = new PureUnionPhysicalTable(Mock(TableName), [baseTable1, baseTable2] as Set)

        expect:
        table.getAvailableIntervals() as List == new SimplifiedIntervalList([
                new Interval('2014-01-01/2014-06-01'),
                new Interval('2015-01-01/2015-06-01'),
                new Interval('2016-01-01/2016-06-01'),
                new Interval('2017-01-01/2017-06-01')
        ] as Set) as List
    }

    def "Creating constrained table throws exception when requesting a column not on the table"() {
        given:
        ConfigPhysicalTable table = new PureUnionPhysicalTable([asName: {'test table'}] as TableName, [baseTable1, baseTable2] as Set)

        DataSourceConstraint constraints = Mock(DataSourceConstraint)
        constraints.allColumnNames >> ['un_configured']

        when:
        table.withConstraint(constraints)

        then:
        RuntimeException exception = thrown()
        exception.message.endsWith('test table')
    }

    def "Creating constrained table then querying the table still unions all subtables"() {
        setup:
        Map logicalColumnNameToInterval = [
                (logicalColumnName1) : new Interval('2014-01-01/2014-06-01'), // on base table 1 and 2
                (logicalColumnName2_1) : new Interval('2015-01-01/2015-06-01'), // on base table 1
                (logicalColumnName2_2) : new Interval('2015-06-01/2016-01-01'), // on base table 2
                (logicalColumnName3) : new Interval('2016-01-01/2016-06-01') // on base table 2
        ] as Map

        baseTable1 = Mock(ConfigPhysicalTable)
        baseTable2 = Mock(ConfigPhysicalTable)
        baseTable1.getSchema() >> schema1
        baseTable2.getSchema() >> schema2

        Availability baseAvailability1 = Mock(Availability)
        Availability baseAvailability2 = Mock(Availability)

        // get available intervals is UNION of intervals of all columns on
        baseAvailability1.getAvailableIntervals() >> {
            return new SimplifiedIntervalList([
                    (Interval) logicalColumnNameToInterval[logicalColumnName1],
                    (Interval) logicalColumnNameToInterval[logicalColumnName2_1]
            ] as Set)
        }

        baseAvailability2.getAvailableIntervals() >> {
            return new SimplifiedIntervalList([
                    (Interval) logicalColumnNameToInterval[logicalColumnName1],
                    (Interval) logicalColumnNameToInterval[logicalColumnName2_2],
                    (Interval) logicalColumnNameToInterval[logicalColumnName3]
            ] as Set)
        }

        // get all available intervals returns mapping of column name to interval list for that logical column
        baseAvailability1.getAllAvailableIntervals() >> {
            [
                    (logicalColumnName1) : new SimplifiedIntervalList([(Interval) logicalColumnNameToInterval[logicalColumnName1]] as Set),
                    (logicalColumnName2_1) : new SimplifiedIntervalList([(Interval) logicalColumnNameToInterval[logicalColumnName2_1]] as Set)
            ] as Map
        }

        baseAvailability2.getAllAvailableIntervals() >> {
            [
                    (logicalColumnName1) : new SimplifiedIntervalList([(Interval) logicalColumnNameToInterval[logicalColumnName1]] as Set),
                    (logicalColumnName2_2) : new SimplifiedIntervalList([(Interval) logicalColumnNameToInterval[logicalColumnName2_2]] as Set),
                    (logicalColumnName3) : new SimplifiedIntervalList([(Interval) logicalColumnNameToInterval[logicalColumnName3]] as Set)
            ] as Map
        }

        //constraint to be used
        DataSourceConstraint constraints = Mock(DataSourceConstraint)
        constraints.getAllColumnNames() >> { [physicalColumnName1, physicalColumnName2, physicalColumnName3] as Set }

        baseAvailability1.getAvailableIntervals(constraints) >> {
            return new SimplifiedIntervalList([
                    (Interval) logicalColumnNameToInterval[logicalColumnName1],
                    (Interval) logicalColumnNameToInterval[logicalColumnName2_1]
            ] as Set)
        }

        baseAvailability2.getAvailableIntervals(constraints) >> {
            return new SimplifiedIntervalList([
                    (Interval) logicalColumnNameToInterval[logicalColumnName1],
                    (Interval) logicalColumnNameToInterval[logicalColumnName2_2],
                    (Interval) logicalColumnNameToInterval[logicalColumnName3]
            ] as Set)
        }

        // Data source name
        DataSourceName dataSourceName1 = Mock(DataSourceName)
        dataSourceName1.asName() >> "foo 1"
        DataSourceName dataSourceName2 = Mock(DataSourceName)
        dataSourceName2.asName() >> "foo 2"
        baseAvailability1.getDataSourceNames(_ as DataSourceConstraint) >> { [dataSourceName1] as Set }
        baseAvailability2.getDataSourceNames(_ as DataSourceConstraint) >> { [dataSourceName2] as Set }

        baseAvailability1.getDataSourceNames() >> { [ dataSourceName1 ] as Set }
        baseAvailability1.getDataSourceNames() >> { [ dataSourceName1 ] as Set }

        baseTable1.getAvailability() >> baseAvailability1
        baseTable2.getAvailability() >> baseAvailability2

        baseTable1.getDataSourceNames() >> { [ dataSourceName1 ] as Set }
        baseTable2.getDataSourceNames() >> { [ dataSourceName1 ] as Set }


        PureUnionPhysicalTable unionTable = new PureUnionPhysicalTable(Mock(TableName), [baseTable1, baseTable2] as Set)
        ConstrainedTable constrainedTable = unionTable.withConstraint(constraints)

        expect:
        constrainedTable.getAvailableIntervals() as List == new SimplifiedIntervalList([
                new Interval('2014-01-01/2014-06-01'),
                new Interval('2015-01-01/2016-06-01')
        ])
        constrainedTable.getDataSourceNames() == ([dataSourceName1, dataSourceName2] as Set)
    }
}
