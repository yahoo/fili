// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Test for metric union availability behavior.
 */
class MetricUnionAvailabilitySpec extends Specification {
    Availability availability1
    Availability availability2

    String metric1
    String metric2

    PhysicalTable physicalTable1
    PhysicalTable physicalTable2

    Set<PhysicalTable> physicalTables

    MetricUnionAvailability metricUnionAvailability

    def setup() {
        availability1 = Mock(Availability)
        availability2 = Mock(Availability)

        availability1.getDataSourceNames() >> ([TableName.of('source1')] as Set)
        availability2.getDataSourceNames() >> ([TableName.of('source2')] as Set)

        metric1 = 'metric1'
        metric2 = 'metric2'

        physicalTable1 = Mock(PhysicalTable)
        physicalTable2 = Mock(PhysicalTable)

        physicalTable1.getAvailability() >> availability1
        physicalTable2.getAvailability() >> availability2

        physicalTables = [physicalTable1, physicalTable2] as Set
    }

    def "Metric columns are initialized by fetching columns from availabilities, not from physical tables"() {
        given:
        availability1.getAllAvailableIntervals() >> [
                (metric1): []
        ]
        availability2.getAllAvailableIntervals() >> [
                (metric2): []
        ]

        MetricColumn tableColumn1 = new MetricColumn("shouldNotBeReturned1")
        MetricColumn tableColumn2 = new MetricColumn("shouldNotBeReturned2")

        metricUnionAvailability = new MetricUnionAvailability(physicalTables, [new MetricColumn(metric1), new MetricColumn(metric2)] as Set)

        when:
        Map<Availability, Set<String>> availabilitiesToAvailableColumns = metricUnionAvailability.availabilitiesToMetricNames

        then:
        availabilitiesToAvailableColumns.size() == 2

        availabilitiesToAvailableColumns.containsKey(availability1)
        Set<String> availableColumns1 = availabilitiesToAvailableColumns.get(availability1)
        availableColumns1.contains(metric1)
        !availableColumns1.contains(tableColumn1)
        !availableColumns1.contains(tableColumn2)

        availabilitiesToAvailableColumns.containsKey(availability2)
        Set<String> availableColumns2 = availabilitiesToAvailableColumns.get(availability2)
        availableColumns2.contains(metric2)
        !availableColumns2.contains(tableColumn1)
        !availableColumns2.contains(tableColumn2)
    }

    @Unroll
    def "checkDuplicateValue returns duplicate metric column names or empty, if no duplicates, from 2 physical tables in the case of #caseDescription"() {
        when:
        boolean actual = MetricUnionAvailability.isMetricUnique(
                [
                        (availability1): metricColumnNames1.collect{it -> it} as Set,
                        (availability2): metricColumnNames2.collect{it -> it} as Set
                ]
        )

        then:
        actual == expected

        where:
        metricColumnNames1     | metricColumnNames2                | expected           | caseDescription
        []                     | []                                | true               | 'both physical tables having empty metric column names (no duplicates)'
        ['metric1']            | []                                | true               | 'one empty table and one non-empty table (no duplicates)'
        ['metric1']            | ['metric1']                       | false              | 'both physical tables having the same metric column names'
        ['metric1', 'metric2'] | ['metric1']                       | false              | 'two tables having intersections'
        ['metric1']            | ['metric2']                       | true               | 'two tables having no intersections (no duplicates)'
        ['metric1', 'metric2'] | ['metric1', 'metric2', 'metric3'] | false              | 'two tables having no intersections (no duplicates)'
    }

    def "constructor throws IllegalArgumentException when 2 availabilities have the same metric column"() {
        given:
        availability1.getAllAvailableIntervals() >> [
                (metric1): []
        ]
        availability2.getAllAvailableIntervals() >> [
                (metric1): []
        ]

        when:
        metricUnionAvailability = new MetricUnionAvailability(physicalTables, [new MetricColumn(metric1)] as Set)

        then:
        RuntimeException exception = thrown()
        exception.message.startsWith("Metric columns must be unique across the metric union data sources, but duplicate was found across the following data sources: source1, source2")
    }


    def "getDataSourceNames returns sources from availabilities not from physical tables"() {
        given:
        availability1.getAllAvailableIntervals() >> [:]
        availability2.getAllAvailableIntervals() >> [:]

        metricUnionAvailability = new MetricUnionAvailability(physicalTables, Collections.emptySet())

        PhysicalTable physicalTable3 = Mock(PhysicalTable)
        physicalTable3.getAvailability() >> metricUnionAvailability
        MetricUnionAvailability outerMetricUnionAvailability = new MetricUnionAvailability(
                [physicalTable3] as Set,
                [] as Set
        )

        expect:
        outerMetricUnionAvailability.getDataSourceNames() == [TableName.of('source1'), TableName.of('source2')] as Set
    }

    def "getAllAvailableIntervals returns the combined intervals of all columns of all availabilities"() {
        given:
        availability1.getAllAvailableIntervals() >> [
                (metric1): [new Interval('2018-01-01/2018-02-01')],
                ('column'): [new Interval('2018-01-01/2018-02-01')]
        ]
        availability2.getAllAvailableIntervals() >> [
                (metric2): [new Interval('2019-01-01/2019-02-01')]
        ]

        metricUnionAvailability = new MetricUnionAvailability(physicalTables, [metric1] as Set)

        expect:
        metricUnionAvailability.getAllAvailableIntervals() == [
                (metric1): [new Interval('2018-01-01/2018-02-01')],
                (metric2): [new Interval('2019-01-01/2019-02-01')],
                'column' : [new Interval('2018-01-01/2018-02-01')]
        ]

    }

    @Unroll
    def "getAvailableIntervals returns the intersection of requested columns when available intervals have #reason"() {
        given:
        availability1.getAllAvailableIntervals() >> [
                (metric1): []
        ]
        availability2.getAllAvailableIntervals() >> [
                (metric2): []
        ]

        availability1.getAvailableIntervals(_ as DataSourceConstraint) >> new SimplifiedIntervalList(
                availableIntervals1.collect{it -> new Interval(it)} as Set
        )
        availability2.getAvailableIntervals(_ as DataSourceConstraint) >> new SimplifiedIntervalList(
                availableIntervals2.collect{it -> new Interval(it)} as Set
        )

        metricUnionAvailability = new MetricUnionAvailability(physicalTables, [new MetricColumn(metric1), new MetricColumn(metric2)] as Set)



        PhysicalDataSourceConstraint dataSourceConstraint = new PhysicalDataSourceConstraint(
                [] as Set,
                [] as Set,
                [] as Set,
                ['metric1', 'metric2'] as Set,
                [] as Set,
                [] as Set,
                [] as Set,
                [:],
                ['metric1', 'metric2'] as Set
        )

        expect:
        metricUnionAvailability.constructSubConstraint(dataSourceConstraint).size() == 2
        metricUnionAvailability.getAvailableIntervals(dataSourceConstraint) == new SimplifiedIntervalList(
                availableIntervals.collect{it -> new Interval(it)} as Set
        )

        where:
        availableIntervals1       | availableIntervals2       | availableIntervals        | reason
        []                        | []                        | []                        | "two empty intervals collections"
        ['2018-01-01/2018-02-01'] | []                        | []                        | "one interval collection being empty"
        ['2017-01-01/2017-02-01'] | ['2017-01-01/2017-02-01'] | ['2017-01-01/2017-02-01'] | "full overlap (start/end, start/end)"
        ['2017-01-01/2017-02-01'] | ['2018-01-01/2018-02-01'] | []                        | "0 overlap (-10/-1, 0/10)"
        ['2017-01-01/2017-02-01'] | ['2017-02-01/2017-03-01'] | []                        | "0 overlap abutting (-10/0, 0/10)"
        ['2017-01-01/2017-02-01'] | ['2017-01-15/2017-03-01'] | ['2017-01-15/2017-02-01'] | "partial front overlap (0/10, 5/15)"
        ['2017-01-01/2017-02-01'] | ['2016-10-01/2017-01-15'] | ['2017-01-01/2017-01-15'] | "partial back overlap (0/10, -5/5)"
        ['2017-01-01/2017-02-01'] | ['2017-01-15/2017-02-01'] | ['2017-01-15/2017-02-01'] | "full front overlap (0/10, 5/10)"
        ['2017-01-01/2017-02-01'] | ['2017-01-01/2017-01-15'] | ['2017-01-01/2017-01-15'] | "full back overlap (0/10, 0/5)"
        ['2017-01-01/2017-02-01'] | ['2017-01-15/2017-01-25'] | ['2017-01-15/2017-01-25'] | "fully contain (0/10, 3/9)"
    }
}
