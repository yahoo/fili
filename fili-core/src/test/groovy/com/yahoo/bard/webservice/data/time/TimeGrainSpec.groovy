// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time

import com.yahoo.bard.webservice.druid.model.query.Granularity

import org.joda.time.DateTime
import org.joda.time.Days
import org.joda.time.Duration
import org.joda.time.Interval
import org.joda.time.Months
import org.joda.time.ReadablePeriod

import spock.lang.Specification
import spock.lang.Unroll

import java.util.stream.Collectors
import java.util.stream.StreamSupport

/**
 * Test default implementation code on TimeGrain.
 */
class TimeGrainSpec extends Specification {

    static class TestConcreteTimeGrain implements TimeGrain {
        ReadablePeriod period;

        TestConcreteTimeGrain(ReadablePeriod period) {
            this.period = period
        }

        @Override
        String getType() {
            return null
        }

        @Override
        String getPeriodString() {
            return null
        }

        @Override
        DateTime roundFloor(DateTime dateTime) {
            return dateTime.monthOfYear().roundFloorCopy();
        }

        @Override
        ReadablePeriod getPeriod() {
            return period
        }

        @Override
        String getName() {
            return null
        }

        @Override
        boolean satisfiedBy(final Granularity that) {
            return false
        }

        @Override
        String getAlignmentDescription() {
            return null
        }

        @Override
        boolean satisfiedBy(final TimeGrain grain) {
            return false
        }

        @Override
        Duration getEstimatedDuration() {
            return null
        }

        @Override
        boolean aligns(final DateTime dateTime) {
            return false
        }
    }

    @Unroll
    def "Test round ceiling rounds from #from to #expected"() {
        TestConcreteTimeGrain timeGrain = new TestConcreteTimeGrain(Months.ONE);
        DateTime fromDate = new DateTime(from)

        expect:
        timeGrain.roundCeiling(fromDate) == new DateTime(expected)

        where:
        from         | expected
        "2015"       | "2015"
        "2014-12-31" | "2015"
        "2015-01-02" | "2015-02"
    }

    @Unroll
    def "Time Grain granularity creates period sliced starting at #expected when dividing #rawIntervals by #period"() {
        TestConcreteTimeGrain timeGrain = new TestConcreteTimeGrain(period)
        List<Interval> expectedIntervals = expected.collect() {
            new Interval(new DateTime(it), period)
        }
        Iterator<Interval> expectedIterator = expectedIntervals.iterator()
        List<Interval> raw = rawIntervals.collect() {
            new Interval(new DateTime(it[0]), it[1])
        }

        when:
        Iterator<Interval> actual = StreamSupport.stream(timeGrain.intervalsIterable(raw).spliterator(), false).
                collect(Collectors.toList()).
                iterator()

        then:
        while(expectedIterator.hasNext()) {
            assert expectedIterator.next() == actual.next()
        }
        ! actual.hasNext()

        where:
        period     | rawIntervals                               | expected
        Days.ONE   | [["2015", Days.THREE]]                     | ["2015-01-01", "2015-01-02", "2015-01-03"]
        Days.THREE | [["2015", Days.THREE]]                     | ["2015-01-01"]
        Days.ONE   | [["2015", Days.THREE], ["2013", Days.ONE]] | ["2013", "2015-01-01", "2015-01-02", "2015-01-03"]
        Days.THREE | [["2015", Days.THREE]]                     | ["2015-01-01"]
    }
}
