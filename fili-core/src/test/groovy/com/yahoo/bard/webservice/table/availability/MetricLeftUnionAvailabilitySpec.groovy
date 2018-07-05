// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.ConfigPhysicalTable
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList
import com.yahoo.bard.webservice.web.filters.ApiFilters

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Test for metric left-union availability behavior.
 */
class MetricLeftUnionAvailabilitySpec extends Specification {
    Availability representativeAvailability
    Availability availability2

    String metric1
    String metric2

    Column metricColumn1
    Column metricColumn2

    ConfigPhysicalTable physicalTable1
    ConfigPhysicalTable physicalTable2

    Set<ConfigPhysicalTable> physicalTables

    MetricLeftUnionAvailability metricLeftUnionAvailability

    ApiFilters apiFilters

    Map<Availability, Set<String>> availabilitiesToMetricNames

    def setup() {
        representativeAvailability = Mock(Availability)
        availability2 = Mock(Availability)

        representativeAvailability.dataSourceNames >> ([DataSourceName.of('source1')] as Set)
        availability2.dataSourceNames >> ([DataSourceName.of('source2')] as Set)

        metric1 = 'metric1'
        metric2 = 'metric2'

        metricColumn1 = new MetricColumn(metric1)
        metricColumn2 = new MetricColumn(metric2)

        physicalTable1 = Mock(ConfigPhysicalTable)
        physicalTable2 = Mock(ConfigPhysicalTable)

        physicalTable1.getAvailability() >> representativeAvailability
        physicalTable2.getAvailability() >> availability2

        physicalTables = [physicalTable1, physicalTable2] as Set

        apiFilters = new ApiFilters()

        availabilitiesToMetricNames = new HashMap<>()
    }

    def "Build method produces the same instance by constructor"() {
        given:
        representativeAvailability.getAllAvailableIntervals() >> [:]
        availability2.getAllAvailableIntervals() >> [:]

        availabilitiesToMetricNames.put(representativeAvailability, [metric1] as Set)
        availabilitiesToMetricNames.put(availability2, [metric2] as Set)

        MetricLeftUnionAvailability availabilityByContr = new MetricLeftUnionAvailability(
                [representativeAvailability] as Set,
                physicalTables.availability as Set,
                availabilitiesToMetricNames
        )
        MetricLeftUnionAvailability availabilityByBuild = MetricLeftUnionAvailability.build(
                [representativeAvailability] as Set,
                physicalTables,
                availabilitiesToMetricNames
        )

        expect:
        availabilityByBuild == availabilityByContr
    }

    @Unroll
    def "getAvailableIntervals returns the intersection of requested columns when available intervals have #reason"() {
        given:
        representativeAvailability.getAllAvailableIntervals() >> [(metric1): []]
        availability2.getAllAvailableIntervals() >> [(metric2): []]

        availabilitiesToMetricNames.put(representativeAvailability, [metric1] as Set)
        availabilitiesToMetricNames.put(availability2, [metric2] as Set)

        representativeAvailability.getAvailableIntervals(_ as PhysicalDataSourceConstraint) >> new SimplifiedIntervalList(
                availableIntervals1.collect{it -> new Interval(it)} as Set
        )
        availability2.getAvailableIntervals(_ as PhysicalDataSourceConstraint) >> new SimplifiedIntervalList(
                availableIntervals2.collect{it -> new Interval(it)} as Set
        )

        metricLeftUnionAvailability = new MetricLeftUnionAvailability(
                [representativeAvailability] as Set,
                physicalTables.availability as Set,
                availabilitiesToMetricNames
        )

        DataSourceConstraint dataSourceConstraint = new DataSourceConstraint(
                [] as Set,
                [] as Set,
                [] as Set,
                [metric1] as Set,
                [] as Set,
                [] as Set,
                [metric1] as Set,
                apiFilters
        )

        PhysicalDataSourceConstraint physicalDataSourceConstraint = new PhysicalDataSourceConstraint(dataSourceConstraint, [metric1, metric2] as Set)

        expect:
        metricLeftUnionAvailability.getAvailableIntervals(physicalDataSourceConstraint) == new SimplifiedIntervalList(
                availableIntervals1.collect{it -> new Interval(it)} as Set
        )

        where:
        availableIntervals1       | availableIntervals2       | reason
        []                        | []                        | "two empty intervals collections"
        ['2018-01-01/2018-02-01'] | []                        | "one interval collection being empty"
        ['2017-01-01/2017-02-01'] | ['2017-01-01/2017-02-01'] | "full overlap (start/end, start/end)"
        ['2017-01-01/2017-02-01'] | ['2018-01-01/2018-02-01'] | "0 overlap (-10/-1, 0/10)"
        ['2017-01-01/2017-02-01'] | ['2017-02-01/2017-03-01'] | "0 overlap abutting (-10/0, 0/10)"
        ['2017-01-01/2017-02-01'] | ['2017-01-15/2017-03-01'] | "partial front overlap (0/10, 5/15)"
        ['2017-01-01/2017-02-01'] | ['2016-10-01/2017-01-15'] | "partial back overlap (0/10, -5/5)"
        ['2017-01-01/2017-02-01'] | ['2017-01-15/2017-02-01'] | "full front overlap (0/10, 5/10)"
        ['2017-01-01/2017-02-01'] | ['2017-01-01/2017-01-15'] | "full back overlap (0/10, 0/5)"
        ['2017-01-01/2017-02-01'] | ['2017-01-15/2017-01-25'] | "fully contain (0/10, 3/9)"
    }

    def "getAvailableIntervals returns empty availability if requesting a column on table that is not supported by the underlying data sources"() {
        given:
        representativeAvailability.getAllAvailableIntervals() >> [(metric1): ['2018-01-01/2018-02-01']]
        availability2.getAllAvailableIntervals() >> [(metric2): ['2018-01-01/2018-02-01']]

        availabilitiesToMetricNames.put(representativeAvailability, [metric1] as Set)
        availabilitiesToMetricNames.put(availability2, [metric2] as Set)

        representativeAvailability.getAvailableIntervals(_ as PhysicalDataSourceConstraint) >> new SimplifiedIntervalList(
                [new Interval('2018-01-01/2018-02-01')]
        )
        availability2.getAvailableIntervals(_ as PhysicalDataSourceConstraint) >> new SimplifiedIntervalList(
                [new Interval('2018-01-01/2018-02-01')]
        )

        metricLeftUnionAvailability = new MetricLeftUnionAvailability(
                [representativeAvailability] as Set,
                physicalTables.availability as Set,
                availabilitiesToMetricNames
        )

        DataSourceConstraint dataSourceConstraint = new DataSourceConstraint(
                [] as Set,
                [] as Set,
                [] as Set,
                [metric1, 'un_configured'] as Set,
                [] as Set,
                [] as Set,
                [metric1, 'un_configured'] as Set,
                apiFilters
        )

        PhysicalDataSourceConstraint physicalDataSourceConstraint = new PhysicalDataSourceConstraint(
                dataSourceConstraint,
                [metric1, metric2, 'un_configured'] as Set
        )

        expect:
        metricLeftUnionAvailability.getAvailableIntervals(physicalDataSourceConstraint) == new SimplifiedIntervalList()
    }

    def "Only representative availability is considered for available interval calculation"() {
        given:
        representativeAvailability.getAllAvailableIntervals() >> [:]
        availability2.getAllAvailableIntervals() >> [:]

        availabilitiesToMetricNames.put(representativeAvailability, [metric1] as Set)
        availabilitiesToMetricNames.put(availability2, [metric2] as Set)

        metricLeftUnionAvailability = new MetricLeftUnionAvailability(
                [representativeAvailability] as Set,
                physicalTables.availability as Set,
                availabilitiesToMetricNames
        )

        expect:
        metricLeftUnionAvailability.getRepresentativeAvailabilitiesToMetricNames() == [
                (representativeAvailability): [metric1] as Set
        ]
    }

    def "getAvailableIntervals() returns available intervals of representative availability"() {
        setup:
        representativeAvailability.getAllAvailableIntervals() >> [(metric1): []]
        availability2.getAllAvailableIntervals() >> [(metric2): []]

        availabilitiesToMetricNames.put(representativeAvailability, [metric1] as Set)
        availabilitiesToMetricNames.put(availability2, [metric2] as Set)

        representativeAvailability.getAvailableIntervals() >> new SimplifiedIntervalList(
                [new Interval('2018-01-01/2018-02-01')]
        )
        availability2.getAvailableIntervals() >> new SimplifiedIntervalList(
                [new Interval('2018-01-01/2018-02-01')]
        )

        metricLeftUnionAvailability = new MetricLeftUnionAvailability(
                [representativeAvailability] as Set,
                physicalTables.availability as Set,
                availabilitiesToMetricNames
        )

        expect:
        metricLeftUnionAvailability.getAvailableIntervals() == new SimplifiedIntervalList(
                [new Interval('2018-01-01/2018-02-01')]
        )
    }

    def "Published references to internal states are immutable"() {
        given:
        representativeAvailability.getAllAvailableIntervals() >> [:]
        availability2.getAllAvailableIntervals() >> [:]

        availabilitiesToMetricNames.put(representativeAvailability, [metric1] as Set)
        availabilitiesToMetricNames.put(availability2, [metric2] as Set)

        metricLeftUnionAvailability = new MetricLeftUnionAvailability(
                [representativeAvailability] as Set,
                physicalTables.availability as Set,
                availabilitiesToMetricNames
        )

        when:
        metricLeftUnionAvailability.getRepresentativeAvailabilities().add(_ as Availability)

        then:
        Exception exception = thrown()
        exception instanceof UnsupportedOperationException
    }

    def "toString matches Javadoc description"() {
        given:
        representativeAvailability.getAllAvailableIntervals() >> [:]
        representativeAvailability.toString() >> "representativeAvailability"
        availability2.getAllAvailableIntervals() >> [:]

        availabilitiesToMetricNames.put(representativeAvailability, [metric1] as Set)
        availabilitiesToMetricNames.put(availability2, [metric2] as Set)

        metricLeftUnionAvailability = new MetricLeftUnionAvailability(
                [representativeAvailability] as Set,
                physicalTables.availability as Set,
                availabilitiesToMetricNames
        )

        expect:
        metricLeftUnionAvailability.toString() ==
                "MetricLeftUnionAvailability{representativeAvailabilities=[representativeAvailability]}"
    }

    def "Left availabilities that are not known to have been configured is considered being missed out"() {
        given:
        Availability missedOut = Mock(Availability) {toString() >> "missedOut"}

        representativeAvailability.getAllAvailableIntervals() >> [:]
        availability2.getAllAvailableIntervals() >> [:]

        availabilitiesToMetricNames.put(representativeAvailability, [metric1] as Set)
        availabilitiesToMetricNames.put(availability2, [metric2] as Set)

        when:
        metricLeftUnionAvailability = new MetricLeftUnionAvailability(
                [representativeAvailability, missedOut] as Set,
                physicalTables.availability as Set,
                availabilitiesToMetricNames
        )

        then:
        Exception exception = thrown()
        exception instanceof IllegalArgumentException
        exception.message == "'[missedOut]' have not been configured in table"
    }
}
