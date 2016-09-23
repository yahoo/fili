// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR

import com.yahoo.bard.webservice.data.time.TimeGrain
import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.druid.model.query.Granularity

import org.joda.time.DateTimeZone
import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

class IntervalUtilsGranularityIteratorSpec extends Specification {

    def setupSpec() {
        DateTimeZone.setDefault(DateTimeZone.UTC);
    }

    def buildListOfIntervalsFromStrings(List<String> intervalStrings) {
        intervalStrings.collect {new Interval(it)}
    }

    @Unroll
    def "Build all time grain GranularityIterator with source: #intervalList"() {
        setup:
        List<Interval> intervals = buildListOfIntervalsFromStrings(intervalList)
        List<Interval> expectedIntervals = buildListOfIntervalsFromStrings(expected)

        Granularity<String> grain = new AllGranularity();
        Iterator iterator = grain.intervalsIterator(intervals)
        expect:
        expectedIntervals.equals( iterator.collect() )

        where:
        intervalList                               | expected
        ["2015/2016"]                              | ["2015/2016"]
        ["2015/2016", "2017/2019"]                 | ["2015/2016", "2017/2019"]
        // Test simplification
        ["2015/2016", "2016/2016", "2017/2019"]    | ["2015/2016", "2017/2019"]
        ["2017/2019", "2015/2016", "2016/2016"]    | ["2015/2016", "2017/2019"]

    }

    @Unroll
    def "Build #timegrain time grain GranularityIterator with source: #intervalList"() {
        setup:
        List<Interval> intervals = buildListOfIntervalsFromStrings(intervalList)
        List<Interval> expectedIntervals = buildListOfIntervalsFromStrings(expected)
        Granularity<TimeGrain> grain = timeGrain

        Iterator iterator = grain.intervalsIterator(intervals)
        expect:
        expectedIntervals.equals( iterator.collect() )

        where:
        timeGrain | intervalList              | expected
        YEAR      | ["2015/2016"]             | ["2015/2016"]
        MONTH     | ["2015-01-01/2015-04-01"] | ["2015-01/2015-02", "2015-02/2015-03", "2015-03/2015-04"]
        DAY       | ["2015-01-01/2015-01-04"] | ["2015-01-01/2015-01-02", "2015-01-02/2015-01-03", "2015-01-03/2015-01-04"]
    }
}
