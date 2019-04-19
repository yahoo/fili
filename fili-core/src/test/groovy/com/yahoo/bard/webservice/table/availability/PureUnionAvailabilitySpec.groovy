// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

class PureUnionAvailabilitySpec extends Specification{

    PureUnionAvailability availability
    Availability sourceAvailability1, sourceAvailability2
    String columnPhysicalName1, columnPhysicalName2
    Interval interval1_1, interval1_2, interval2_1, interval2_2

    def setup() {
        columnPhysicalName1 = 'column_one'
        columnPhysicalName2 = 'column_two'

        interval1_1 = new Interval('2015-01-01/2016-01-01')
        interval1_2 = new Interval('2016-01-01/2017-01-01')

        interval2_1 = new Interval('2014-01-01/2015-01-01')
        interval2_2 = new Interval('2017-01-01/2018-01-01')

        sourceAvailability1 = Mock(Availability)
        sourceAvailability1.getAllAvailableIntervals() >> {
            [
                (columnPhysicalName1): [interval1_1] as SimplifiedIntervalList,
                (columnPhysicalName2): [interval1_2] as SimplifiedIntervalList
            ]
        }
        sourceAvailability1.getAvailableIntervals() >> { [interval1_1, interval1_2] as SimplifiedIntervalList }

        sourceAvailability2 = Mock(Availability)
        sourceAvailability2.getAllAvailableIntervals() >> {
            [
                (columnPhysicalName1): [interval2_1] as SimplifiedIntervalList,
                (columnPhysicalName2): [interval2_2] as SimplifiedIntervalList
            ]
        }
        sourceAvailability2.getAvailableIntervals() >> { [interval2_1, interval2_2] as SimplifiedIntervalList }

        availability = new PureUnionAvailability([sourceAvailability1, sourceAvailability2] as Set)
    }

    def "getAllAvailableIntervals returns all availabilities for all columns bound by the timeFilterInterval" () {
        expect:
        availability.getAllAvailableIntervals() == [
                (columnPhysicalName1): [new Interval('2014-01-01/2016-01-01')],
                (columnPhysicalName2): [new Interval('2016-01-01/2018-01-01')]
        ] as LinkedHashMap
    }

    def "getAvailableIntervals with NO constraint properly unions all intervals from target interval together"() {
        expect:
        availability.getAvailableIntervals() == [
                new Interval('2014-01-01/2018-01-01')
        ] as SimplifiedIntervalList
    }

    @Unroll
    def "getAvailableIntervals returns the union of the requested column available intervals when there is #description"() {
        given:
        interval1_1 = new Interval(firstInterval)
        interval1_2 = new Interval(firstInterval)

        interval2_1 = new Interval(secondInterval)
        interval2_2 = new Interval(secondInterval)

        PhysicalDataSourceConstraint dataSourceConstraint = Mock(PhysicalDataSourceConstraint)
        dataSourceConstraint.allColumnPhysicalNames >> [columnPhysicalName1, columnPhysicalName2]

        sourceAvailability1 = Mock(StrictAvailability)
        sourceAvailability1.getAvailableIntervals(dataSourceConstraint) >> {
            [
                    new SimplifiedIntervalList([interval1_1, interval1_2] as Set)
            ]
        }

        sourceAvailability2 = Mock(StrictAvailability)
        sourceAvailability2.getAvailableIntervals(dataSourceConstraint) >> {
            [
                    new SimplifiedIntervalList([interval2_1, interval2_2] as Set)
            ]
        }

        availability = new PureUnionAvailability([sourceAvailability1, sourceAvailability2] as Set)

        expect:
        availability.getAvailableIntervals(dataSourceConstraint) == new SimplifiedIntervalList(
                expected.collect{new Interval(it)} as Set
        )

        //TODO FINISH THIS
        where:
        firstInterval           | secondInterval          | expected                                            | description
        '2017-01-01/2017-06-01' | '2017-06-01/2018-01-01' | ['2017-01-01/2018-01-01']                           | "abutting intervals, should merge into one large interval"
        '2017-01-01/2017-06-01' | '2018-01-01/2018-06-01' | ['2017-01-01/2017-06-01', '2018-01-01/2018-06-01']  | "no overlap, should be two separate intervals "
        '2017-01-01/2018-01-01' | '2017-06-01/2018-06-01' | ['2017-01-01/2018-06-01']                           | "some overlap, should just be unioned into one large interval "
        '2017-01-01/2018-01-01' | '2017-07-01/2018-01-01' | ['2017-01-01/2018-01-01']                           | "same interval, should just merged into a single interval with the same endpoints"
    }

    def "PureUnionAvailability explicitly implements getAvailableIntervals and doesn't implement the PhysicalDataSourceConstraint signature removed from the interface"() {
        setup:
        Availability mockAvailability = Mock(Availability)
        PureUnionAvailability pureUnionAvailability = new PureUnionAvailability([mockAvailability] as Set)

        SimplifiedIntervalList simplifiedIntervalList = new SimplifiedIntervalList()
        PhysicalDataSourceConstraint physicalDataSourceConstraint = Mock(PhysicalDataSourceConstraint)

        when:
        pureUnionAvailability.getAvailableIntervals((DataSourceConstraint) physicalDataSourceConstraint)

        and: "PhysicalDataSourceConstraint formerly had it's own signature which might still be implemented in subclasses"
        pureUnionAvailability.getAvailableIntervals(physicalDataSourceConstraint)

        then: "Calls hit the method signature from the interface"
        2 * mockAvailability.getAvailableIntervals(_ as DataSourceConstraint) >> simplifiedIntervalList

        and: "Both paths hit explicit methods, not interface default implementation"
        0 * mockAvailability.getAvailableIntervals()
    }
}
