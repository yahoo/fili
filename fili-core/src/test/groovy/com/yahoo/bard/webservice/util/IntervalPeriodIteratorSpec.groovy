// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.joda.time.Period

import spock.lang.Specification
import spock.lang.Unroll

class IntervalPeriodIteratorSpec extends Specification {

    def setupSpec() {
        DateTimeZone.setDefault(DateTimeZone.UTC);
    }

    @Unroll
    def "boundary at returns Date #expectedMoment with period #period and n #n from interval #baseInterval"() {
        expect:
        boundaryAtCalculationIsCorrect(baseInterval, period, n, expectedInstant)

        where:
        baseInterval                 | period |  n  || expectedInstant
        "2014/2020"                  | "P1D"  |  1  || "2014-01-02T00:00:00.000Z"
        "2014/2020"                  | "PT1H" |  1  || "2014-01-01T01:00:00.000Z"
    }

    @Unroll
    def "boundaryAt is aligned to interval start for #baseInterval with period #period"() {
        expect:
        boundaryAtCalculationIsCorrect(baseInterval, period, n, expectedInstant)

        where:
        baseInterval                               | period |  n  || expectedInstant
        "2014/2020"                                | "P1W"  |  1  || "2014-01-08T00:00:00.000Z"
        "2014-01-06T01:01:00.000Z/2014-02-06"      | "P5D"  |  2  || "2014-01-16T01:01:00.000Z"
    }

    @Unroll
    def "boundaryAt is preserved relative to grain period #period even with ragged edge with interval #baseInterval"() {
        expect:
        boundaryAtCalculationIsCorrect(baseInterval, period, n, expectedInstant)

        where:
        baseInterval           | period |  n  || expectedInstant
        "2014-01-31/2020"      | "P1M"  |  1  || "2014-02-28T00:00:00.000Z"
        "2014-01-31/2020"      | "P1M"  |  2  || "2014-03-31T00:00:00.000Z"
        "2014-01-31/2020"      | "P1M"  |  3  || "2014-04-30T00:00:00.000Z"
    }

    @Unroll
    def "#does have next for #baseInterval with #period when at #adjustedPosition"() {
        setup:
        IntervalPeriodIterator underTest = getIteratorToTest(period, baseInterval)
        underTest.currentPosition = new DateTime(adjustedPosition)

        expect:
        underTest.hasNext() == expected

        where:
        baseInterval         | period | adjustedPosition || expected
        "2014/2016"          | "P1Y"  | "2016"           || false
        "2014/2016"          | "P1Y"  | "2017"           || false
        "2014/2016"          | "P1Y"  | "2015"           || true

        does = expected ? "Does " : "Does not"
    }

    def "Iterator returns hasNext and then finishes"() {
        given:
        Period p = new Period("P1Y")
        Interval base = new Interval("2014/2016")

        when:
        IntervalPeriodIterator underTest = new IntervalPeriodIterator(p, base)

        then:
        underTest.hasNext()

        then:
        underTest.next() == new Interval("2014/2015")

        then:
        underTest.next() == new Interval("2015/2016")

        then:
        ! underTest.hasNext()
    }

    def "Next for uneven intervals returns a truncated period"() {
        when:
        IntervalPeriodIterator underTest = getIteratorToTest("P1Y", "2014/2015-02-01")

        then:
        underTest.hasNext()
        underTest.next() == new Interval("2014/2015")

        then:
        underTest.hasNext()
        underTest.next() == new Interval("2015/2015-02-01")

        then:
        ! underTest.hasNext()
    }

    def "Next fails if next called when it doesn't have next"() {
        given:
        IntervalPeriodIterator underTest = getIteratorToTest("P1Y", "2014/2015")
        underTest.next()

        expect:
        ! underTest.hasNext()

        when:
        underTest.next()

        then:
        thrown(NoSuchElementException)
    }

    IntervalPeriodIterator getIteratorToTest(String period, String baseInterval) {
        new IntervalPeriodIterator(new Period(period), new Interval(baseInterval))
    }

    void boundaryAtCalculationIsCorrect(String baseInterval, String period, int n, String expectedInstant) {
        IntervalPeriodIterator underTest = new IntervalPeriodIterator(new Period(period), new Interval(baseInterval))
        assert underTest.boundaryAt(n) == new DateTime(expectedInstant)
    }
}
