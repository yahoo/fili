// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Test for time filtered availability behavior.
 */
class TimeFilteredAvailabilitySpec extends Specification {

    Availability sourceAvailability
    TimeFilteredAvailability timeFilteredAvailability
    String columnPhysicalName1, columnPhysicalName2, columnPhysicalName3
    SimplifiedIntervalList timeFilterIntervals
    Interval interval1, interval2, interval3

    def setup() {
        columnPhysicalName1 = 'column_one'
        columnPhysicalName2 = 'column_two'
        columnPhysicalName3 = 'column_three'

        timeFilterIntervals = new SimplifiedIntervalList(Collections.singletonList(new Interval('2015-01-01/2017-01-01')))

        interval1 = new Interval('2015-01-01/2016-01-01')
        interval2 = new Interval('2016-01-01/2018-01-01')
        interval3 = new Interval('2018-01-01/2019-01-01')

        sourceAvailability = Mock(Availability)
        sourceAvailability.getAllAvailableIntervals() >> {
            [
                    (columnPhysicalName1): [interval1] as SimplifiedIntervalList,
                    (columnPhysicalName2): [interval2] as SimplifiedIntervalList,
                    (columnPhysicalName3): [interval3] as SimplifiedIntervalList
            ]
        }
        sourceAvailability.getAvailableIntervals() >> {
            [new Interval('2015-01-01/2019-01-01')] as SimplifiedIntervalList
        }

        timeFilteredAvailability = new TimeFilteredAvailability(
                sourceAvailability,
                { timeFilterIntervals }
        )
    }

    def "getAllAvailableIntervals returns all availabilities for all columns bound by the timeFilterInterval" () {
        setup:
        Interval expectedInterval2 = new Interval('2016-01-01/2017-01-01')

        expect:
        timeFilteredAvailability.getAllAvailableIntervals() == [
                (columnPhysicalName1): [interval1],
                (columnPhysicalName2): [expectedInterval2],
                (columnPhysicalName3): []
        ] as LinkedHashMap
    }

    def "getAvailableIntervals with NO constraint properly delegates to target and THEN intersects the result with the filter interval"() {
        expect:
        // source available intervals do not match with the time filters available intervals, as the source has intervals outside of the time filter
        timeFilteredAvailability.getAvailableIntervals() != sourceAvailability.getAvailableIntervals()
        // now that the source available intervals are intersected with the time filter intervals, the result should match the TimeFilterAvailability's available intervals
        timeFilteredAvailability.getAvailableIntervals() == sourceAvailability.getAvailableIntervals().intersect(timeFilterIntervals)
    }

    @Unroll
    def "getAvailableIntervals returns the intersection of the requested column available intervals with the filter interval when there is #description"() {
        given:
        interval1 = new Interval(firstInterval)
        interval2 = new Interval(secondInterval)

        PhysicalDataSourceConstraint dataSourceConstraint = Mock(PhysicalDataSourceConstraint)
        dataSourceConstraint.allColumnPhysicalNames >> [columnPhysicalName1, columnPhysicalName2]

        sourceAvailability = Mock(StrictAvailability)
        sourceAvailability.getAvailableIntervals(dataSourceConstraint) >> {
            [
                    new SimplifiedIntervalList([interval1]).union(new SimplifiedIntervalList([interval2]))
            ]
        }

        timeFilteredAvailability = new TimeFilteredAvailability(
                sourceAvailability,
                { timeFilterIntervals }
        )

        expect:
        timeFilteredAvailability.getAvailableIntervals(dataSourceConstraint) == new SimplifiedIntervalList(
                expected.collect{new Interval(it)} as Set
        )

        // time filter: '2015-01-01/2017-01-01'
        where:
        firstInterval           | secondInterval          | expected                   | description
        '2017-01-01/2017-06-01' | '2017-06-01/2018-01-01' | []                         | "there is no overlap, time filter is before union of intervals"
        '2016-01-01/2017-06-01' | '2018-01-01/2019-01-01' | ['2016-01-01/2017-01-01']  | "time filter overlaps with beginning of first interval in union"
        '2015-06-01/2016-01-01' | '2016-01-01/2018-01-01' | ['2015-06-01/2017-01-01']  | "time filter overlaps entire first interval and part of second interval"
        '2014-06-01/2017-06-01' | '2017-06-01/2018-01-01' | ['2015-01-01/2017-01-01']  | "time filter is completely contained by first interval"
        '2014-01-01/2015-06-01' | '2015-06-01/2018-01-01' | ['2015-01-01/2017-01-01']  | "time filter is by the union of both intervals"
        '2014-01-01/2014-06-01' | '2014-06-01/2018-01-01' | ['2015-01-01/2017-01-01']  | "time filter is completely contained by second interval"
        '2014-01-01/2015-06-01' | '2015-06-01/2016-06-01' | ['2015-01-01/2016-06-01']  | "time filter begins in the middle of the first interval and extends past second interval"
        '2014-01-01/2014-06-01' | '2014-06-01/2016-06-01' | ['2015-01-01/2016-06-01']  | "time filter begins in second interval and extends past it"
        '2014-01-01/2014-06-01' | '2014-06-01/2015-01-01' | []                         | "there is no overlap, time filter is after union of intervals"
    }
}
