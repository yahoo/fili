// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR

import org.joda.time.DateTime
import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

class DateTimeUtilsSpec extends Specification {

    @Unroll
    def "check if #c = #a + #b"() {
        expect:
        DateTimeUtils.addTimeGrain(new DateTime(a), b) == new DateTime(c)

        where:
        a                         | b     || c
        "2014-01-01T10:00:00.000" | HOUR  || "2014-01-01T11:00:00.000"
        "2014-01-01"              | DAY   || "2014-01-02"
        "2014-01-31"              | DAY   || "2014-02-01"
        "2014-04-30"              | DAY   || "2014-05-01"
        "2012-02-29"              | DAY   || "2012-03-01"
        "2014-02-28"              | DAY   || "2014-03-01"
        "2012-02-28"              | DAY   || "2012-02-29"
        "2014-02-28"              | WEEK  || "2014-03-07"
        "2012-02-28"              | WEEK  || "2012-03-06"
        "2014-01-01"              | WEEK  || "2014-01-08"
        "2014-02-01"              | WEEK  || "2014-02-08"
        "2014-01-01"              | MONTH || "2014-02-01"
        "2014-02-01"              | MONTH || "2014-03-01"
        "2014-04-01"              | MONTH || "2014-05-01"
        "2012-02-01"              | MONTH || "2012-03-01"
        "2012-01-01"              | YEAR  || "2013-01-01"
        "2014-07-01"              | YEAR  || "2015-07-01"
    }

    def buildIntervalSet(List<String> intervalStrList) {
        Set<Interval> intervals = new TreeSet<>(new IntervalStartComparator())

        intervalStrList.each {
            intervals.add(new Interval(it))
        }

        intervals
    }

    @Unroll
    def "check for proper merging of #b into given interval set"() {
        expect:
        DateTimeUtils.mergeIntervalToSet(buildIntervalSet(a), new Interval(b)) == buildIntervalSet(c)

        where:
        // CHECKSTYLE:OFF
        a                                                                           | b                       || c
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15"] | "2014-01-09/2014-01-10" || ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-09/2014-01-10", "2014-01-12/2014-01-15"]
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15"] | "2014-01-07/2014-01-09" || ["2014-01-01/2014-01-03", "2014-01-05/2014-01-09", "2014-01-12/2014-01-15"]
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15"] | "2014-01-06/2014-01-09" || ["2014-01-01/2014-01-03", "2014-01-05/2014-01-09", "2014-01-12/2014-01-15"]
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15"] | "2014-01-06/2014-01-14" || ["2014-01-01/2014-01-03", "2014-01-05/2014-01-15"]
        ["2014-01-01/2014-01-03", "2014-01-05/2014-01-07", "2014-01-12/2014-01-15"] | "2014-01-02/2014-01-14" || ["2014-01-01/2014-01-15"]
        // CHECKSTYLE:ON
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
}
