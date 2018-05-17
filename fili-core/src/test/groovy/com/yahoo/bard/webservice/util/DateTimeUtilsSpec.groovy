// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MINUTE
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.QUARTER
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

class DateTimeUtilsSpec extends Specification {

    @Unroll
    def "check if #sum = #dateTime + #timeGrain"() {
        expect:
        DateTimeUtils.addTimeGrain(new DateTime(dateTime), timeGrain) == new DateTime(sum)

        where:
        dateTime                  | timeGrain | sum
        "2014-01-01T10:00:00.000" | HOUR      | "2014-01-01T11:00:00.000"
        "2014-01-01"              | DAY       | "2014-01-02"
        "2014-01-31"              | DAY       | "2014-02-01"
        "2014-04-30"              | DAY       | "2014-05-01"
        "2012-02-29"              | DAY       | "2012-03-01"
        "2014-02-28"              | DAY       | "2014-03-01"
        "2012-02-28"              | DAY       | "2012-02-29"
        "2014-02-28"              | WEEK      | "2014-03-07"
        "2012-02-28"              | WEEK      | "2012-03-06"
        "2014-01-01"              | WEEK      | "2014-01-08"
        "2014-02-01"              | WEEK      | "2014-02-08"
        "2014-01-01"              | MONTH     | "2014-02-01"
        "2014-02-01"              | MONTH     | "2014-03-01"
        "2014-04-01"              | MONTH     | "2014-05-01"
        "2012-02-01"              | MONTH     | "2012-03-01"
        "2012-01-01"              | YEAR      | "2013-01-01"
        "2014-07-01"              | YEAR      | "2015-07-01"
    }

    @Unroll
    def "check for proper merging of #intervals into a set"() {
        expect:
        DateTimeUtils.mergeIntervalSet(buildIntervalSet(intervals)) == buildIntervalSet(merged)

        where:
        // CHECKSTYLE:OFF
        intervals                                                                                            | merged
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15", "2014-01-09/2014-01-10"] | ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-09/2014-01-10", "2014-01-12/2014-01-15"]
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15", "2014-01-07/2014-01-09"] | ["2014-01-01/2014-01-03", "2014-01-05/2014-01-09", "2014-01-12/2014-01-15"]
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15", "2014-01-06/2014-01-09"] | ["2014-01-01/2014-01-03", "2014-01-05/2014-01-09", "2014-01-12/2014-01-15"]
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15", "2014-01-06/2014-01-14"] | ["2014-01-01/2014-01-03", "2014-01-05/2014-01-15"]
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15", "2014-01-02/2014-01-14"] | ["2014-01-01/2014-01-15"]
        // CHECKSTYLE:ON
    }

    @Unroll
    def "check for proper merging of #singleInterval into given interval set"() {
        expect:
        DateTimeUtils.mergeIntervalToSet(buildIntervalSet(intervals), new Interval(singleInterval)) == buildIntervalSet(merged)

        where:
        // CHECKSTYLE:OFF
        intervals                                                                   | singleInterval          | merged
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15"] | "2014-01-09/2014-01-10" | ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-09/2014-01-10", "2014-01-12/2014-01-15"]
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15"] | "2014-01-07/2014-01-09" | ["2014-01-01/2014-01-03", "2014-01-05/2014-01-09", "2014-01-12/2014-01-15"]
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15"] | "2014-01-06/2014-01-09" | ["2014-01-01/2014-01-03", "2014-01-05/2014-01-09", "2014-01-12/2014-01-15"]
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15"] | "2014-01-06/2014-01-14" | ["2014-01-01/2014-01-03", "2014-01-05/2014-01-15"]
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15"] | "2014-01-02/2014-01-14" | ["2014-01-01/2014-01-15"]
        // CHECKSTYLE:ON
    }

    @Unroll
    def "findFullAvailabilityGaps returns the gaps, #gaps, in #available vs #needed interval sets in the case of #description"() {
        expect:
        DateTimeUtils.findFullAvailabilityGaps(buildIntervalSet(available), buildIntervalSet(needed)) ==
                buildIntervalSet(gaps) as SortedSet

        where:
        available                                          | needed                                             | gaps                                               | description
        ["2014-01-01/2014-01-03"]                          | ["2014-01-01/2014-01-03"]                          | []                                                 | "available interval = needed interval"
        ["2014-01-01/2014-01-03", "2014-01-07/2014-01-08"] | ["2014-01-01/2014-01-03", "2014-01-07/2014-01-08"] | []                                                 | "available intervals = needed intervals"
        ["2014-01-01/2014-01-03"]                          | ["2014-01-05/2014-01-06"]                          | ["2014-01-05/2014-01-06"]                          | "no overlap"
        ["2014-01-01/2014-01-03", "2014-01-04/2014-01-05"] | ["2014-01-07/2014-01-08", "2014-01-09/2014-01-10"] | ["2014-01-07/2014-01-08", "2014-01-09/2014-01-10"] | "no overlaps"
        ["2014-01-01/2014-01-03"]                          | ["2014-01-02/2014-01-05"]                          | ["2014-01-02/2014-01-05"]                          | "overlap between available right and needed left"
        ["2014-01-01/2014-01-03"]                          | ["2014-01-02/2014-01-05", "2014-01-02/2014-01-04"] | ["2014-01-02/2014-01-05", "2014-01-02/2014-01-04"] | "overlaps between available right and needed left"
        ["2014-01-01/2014-01-03", "2014-01-03/2014-01-04"] | ["2014-01-02/2014-01-05"]                          | ["2014-01-02/2014-01-05"]                          | "overlaps between available right and needed left"
        ["2014-01-02/2014-01-05"]                          | ["2014-01-01/2014-01-03"]                          | ["2014-01-01/2014-01-03"]                          | "overlap between available left and needed right"
        ["2014-01-02/2014-01-05"]                          | ["2014-01-01/2014-01-03", "2014-01-01/2014-01-04"] | ["2014-01-01/2014-01-03", "2014-01-01/2014-01-04"] | "overlap between available left and needed right"
        ["2014-01-02/2014-01-03", "2014-01-02/2014-01-05"] | ["2014-01-01/2014-01-03"]                          | ["2014-01-01/2014-01-03"]                          | "overlap between available left and needed right"
        ["2014-01-01/2014-01-03"]                          | ["2014-01-03/2014-01-04"]                          | ["2014-01-03/2014-01-04"]                          | "available right abuts needed left"
        ["2014-01-02/2014-01-03"]                          | ["2014-01-01/2014-01-02"]                          | ["2014-01-01/2014-01-02"]                          | "available left abuts needed right"
        ["2014-01-01/2014-01-05"]                          | ["2014-01-02/2014-01-03"]                          | []                                                 | "available interval contain needed interval"
        ["2014-01-01/2014-01-03", "2014-01-02/2014-01-05"] | ["2014-01-02/2014-01-03"]                          | []                                                 | "available intervals contain needed interval"
        ["2014-01-01/2014-01-05"]                          | ["2014-01-02/2014-01-03"]                          | []                                                 | "available interval contain needed interval"
        ["2014-01-01/2014-01-03", "2014-01-02/2014-01-05"] | ["2014-01-02/2014-01-03", "2014-01-04/2014-01-05"] | []                                                 | "available intervals contain needed intervals"
    }

    @Unroll
    def "Verify if intervalToString() method formats interval #a to string #b"() {
        expect:
        DateTimeUtils.intervalToString(new Interval(a), DateTimeFormatterFactory.getOutputFormatter(), "/") == b

        where:
        a                                                 || b
        "2014-07-01T10:00:00.000/2014-07-01T11:00:00.000" || "2014-07-01 10:00:00.000/2014-07-01 11:00:00.000"
        "2014-07-01/2014-07-02"                           || "2014-07-01 00:00:00.000/2014-07-02 00:00:00.000"
    }

    def "sliceIntervals throws IllegalArgumentException when interval is not aligned with time grain"() {
        given:
        Interval interval = new Interval("2014-07-05/2014-07-31")

        when:
        DateTimeUtils.sliceIntervals(interval, MONTH)

        then:
        Exception exception = thrown()
        exception instanceof IllegalArgumentException
        ("Interval must be aligned to the TimeGrain starting " + MONTH.roundFloor(interval.start)).equals(
                exception.message
        )
    }

    def "sliceIntervals throws IllegalArgumentException when interval is not a multiple of time grain"() {
        given:
        Interval interval = new Interval("2014-07-01/2014-12-10")

        when:
        DateTimeUtils.sliceIntervals(interval, MONTH)

        then:
        Exception exception = thrown()
        exception instanceof IllegalArgumentException
        "Interval must be a multiple of the TimeGrain".equals(exception.message)
    }

    @Unroll
    def "sliceIntervals slices #interval into #expected of the timeGrain #timeGrain"() {
        expect:
        DateTimeUtils.sliceIntervals(new Interval(interval), timeGrain)  == (buildIntervalSet(expected) as List)

        where:
        interval                                          | timeGrain | expected
        "2014-07-01T00:00:00.000/2014-07-01T00:02:00.000" | MINUTE    | ["2014-07-01T00:00:00.000/2014-07-01T00:01:00.000", "2014-07-01T00:01:00.000/2014-07-01T00:02:00.000"]
        "2014-07-01T00:00:00.000/2014-07-01T02:00:00.000" | HOUR      | ["2014-07-01T00:00:00.000/2014-07-01T01:00:00.000", "2014-07-01T01:00:00.000/2014-07-01T02:00:00.000"]
        "2014-07-01T00:00:00.000/2014-07-03T00:00:00.000" | DAY       | ["2014-07-01T00:00:00.000/2014-07-02T00:00:00.000", "2014-07-02T00:00:00.000/2014-07-03T00:00:00.000"]
        "2014-06-30T00:00:00.000/2014-07-14T00:00:00.000" | WEEK      | ["2014-06-30T00:00:00.000/2014-07-07T00:00:00.000", "2014-07-07T00:00:00.000/2014-07-14T00:00:00.000"]
        "2014-07-01T00:00:00.000/2014-09-01T00:00:00.000" | MONTH     | ["2014-07-01T00:00:00.000/2014-08-01T00:00:00.000", "2014-08-01T00:00:00.000/2014-09-01T00:00:00.000"]
        "2014-07-01T00:00:00.000/2015-01-01T00:00:00.000" | QUARTER   | ["2014-07-01T00:00:00.000/2014-10-01T00:00:00.000", "2014-10-01T00:00:00.000/2015-01-01T00:00:00.000"]
        "2014-01-01T00:00:00.000/2016-01-01T00:00:00.000" | YEAR      | ["2014-01-01T00:00:00.000/2015-01-01T00:00:00.000", "2015-01-01T00:00:00.000/2016-01-01T00:00:00.000"]
    }

    @Unroll
    def "The start of the quarter containing #datetime is #startOfQuarter"() {
        expect:
        DateTimeUtils.quarterlyRound(datetime) == startOfQuarter

        where:
        datetime                         | startOfQuarter
        //Quarter 1
        new DateTime(2016, 01, 01, 0, 0) | new DateTime(2016, 01, 01, 0, 0)
        new DateTime(2016, 01, 01, 5, 0) | new DateTime(2016, 01, 01, 0, 0)
        new DateTime(2016, 01, 03, 5, 0) | new DateTime(2016, 01, 01, 0, 0)
        new DateTime(2016, 02, 01, 0, 0) | new DateTime(2016, 01, 01, 0, 0)
        new DateTime(2016, 02, 03, 5, 0) | new DateTime(2016, 01, 01, 0, 0)
        new DateTime(2016, 03, 31, 0, 0) | new DateTime(2016, 01, 01, 0, 0)
        //Quarter 2
        new DateTime(2016, 04, 01, 0, 0) | new DateTime(2016, 04, 01, 0, 0)
        new DateTime(2016, 04, 01, 5, 0) | new DateTime(2016, 04, 01, 0, 0)
        new DateTime(2016, 04, 03, 5, 0) | new DateTime(2016, 04, 01, 0, 0)
        new DateTime(2016, 05, 01, 0, 0) | new DateTime(2016, 04, 01, 0, 0)
        new DateTime(2016, 05, 03, 5, 0) | new DateTime(2016, 04, 01, 0, 0)
        new DateTime(2016, 06, 30, 0, 0) | new DateTime(2016, 04, 01, 0, 0)
    }

    def "A ZonedTimeGrain produces a correct time zone"() {
        given:
        ZonedTimeGrain granularity = Mock(ZonedTimeGrain)
        granularity.getTimeZone() >> DateTimeZone.UTC

        expect:
        DateTimeUtils.getTimeZone(granularity) == DateTimeZone.UTC
    }

    def "A granularity that is not a ZonedTimeGrain produces a default timezone"() {
        expect:
        DateTimeUtils.getTimeZone(DefaultTimeGrain.HOUR) == DateTimeZone.default
    }

    def buildIntervalSet(List<String> intervalStrList) {
        Set<Interval> intervals = new TreeSet<>(new IntervalStartComparator())

        intervalStrList.each {
            intervals.add(new Interval(it))
        }

        intervals
    }
}
