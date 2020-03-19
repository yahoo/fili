// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.QUARTER
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.GranularityParser
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.data.time.TimeGrain
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.util.DateTimeFormatterFactory
import com.yahoo.bard.webservice.util.IntervalUtils
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.apirequest.utils.TestingDataApiRequestImpl

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.joda.time.format.DateTimeFormatter

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.function.Function

class DataApiRequestIntervalsSpec extends Specification {

    @Shared
    DimensionDictionary dimensionDict
    @Shared
    MetricDictionary metricDict
    @Shared
    LogicalTable table

    static GranularityParser granularityParser = new StandardGranularityParser()
    DateTimeFormatter dateTimeFormatter

    @Shared
    Function<TimeGrain, DateTime> dateParser = { new TestingDataApiRequestImpl().getCurrentDate(new DateTime(), it)}

    static final DateTimeZone originalTimeZone = DateTimeZone.default

    TestingDataApiRequestImpl apiRequest = new TestingDataApiRequestImpl()

    def setupSpec() {
        DateTimeZone.default = IntervalUtils.SYSTEM_ALIGNMENT_EPOCH.zone
    }

    def setup() {
        dateTimeFormatter = DateTimeFormatterFactory.FULLY_OPTIONAL_DATETIME_FORMATTER.withZone(DateTimeZone.default)
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        dimensionDict = new DimensionDictionary()
        KeyValueStoreDimension keyValueStoreDimension
        [ "locale", "one", "two", "three" ].each { String name ->
            keyValueStoreDimension = new KeyValueStoreDimension(name, "druid-"+name, dimensionFields, MapStoreManager.getInstance(name), ScanSearchProviderManager.getInstance(name))
            keyValueStoreDimension.setLastUpdated(new DateTime(10000))
            dimensionDict.add(keyValueStoreDimension)
        }

        metricDict = new MetricDictionary()
        [ "met1", "met2", "met3", "met4" ].each { String name ->
            metricDict.put(name, new LogicalMetricImpl(null, null, name))
        }
        TableGroup tg = Mock(TableGroup)
        tg.getDimensions() >> dimensionDict.apiNameToDimension.values()
        tg.getApiMetricNames() >> ([] as Set)
        table = new LogicalTable("name", DAY, tg, metricDict)
    }

    def cleanupSpec() {
        DateTimeZone.default = originalTimeZone
    }

    @Unroll
    def "check parsing interval #intervalString parses to #parsedStart/#parsedStop"() {

        given: "An expected interval"
        Interval expectedInterval = new Interval(parsedStart, parsedStop)

        expect: "The interval string parses into a single interval"
        new TestingDataApiRequestImpl().generateIntervals(
                intervalString,
                apiRequest.generateGranularity(name, granularityParser), dateTimeFormatter
        ).size() == 1

        and: "It parses to the interval we expect"
        new TestingDataApiRequestImpl().generateIntervals(
                intervalString,
                apiRequest.generateGranularity(name, granularityParser), dateTimeFormatter
        ).first() == expectedInterval

        where:
        intervalString                                    | name  | parsedStart                                  | parsedStop
        "2005/2006"                                       | "day" | new DateTime(2005, 01, 01, 00, 00, 00, 000)  | new DateTime(2006, 01, 01, 00, 00, 00, 000)
        "2005-03/2005-04"                                 | "day" | new DateTime(2005, 03, 01, 00, 00, 00, 000)  | new DateTime(2005, 04, 01, 00, 00, 00, 000)
        "2005-03-25/2005-03-26"                           | "day" | new DateTime(2005, 03, 25, 00, 00, 00, 000)  | new DateTime(2005, 03, 26, 00, 00, 00, 000)
        "2005-03-25T10/2005-03-25T11"                     | "day" | new DateTime(2005, 03, 25, 10, 00, 00, 000)  | new DateTime(2005, 03, 25, 11, 00, 00, 000)
        "2005-03-25T10:20/2005-03-25T10:21"               | "day" | new DateTime(2005, 03, 25, 10, 20, 00, 000)  | new DateTime(2005, 03, 25, 10, 21, 00, 000)
        "2005-03-25T10:20:30/2005-03-25T10:20:31"         | "day" | new DateTime(2005, 03, 25, 10, 20, 30, 000)  | new DateTime(2005, 03, 25, 10, 20, 31, 000)
        "2005-03-25T10:20:30.555/2005-03-25T10:20:30.556" | "day" | new DateTime(2005, 03, 25, 10, 20, 30, 555)  | new DateTime(2005, 03, 25, 10, 20, 30, 556)

        "2005-03-25 10/2005-03-25 11"                     | "day" | new DateTime(2005, 03, 25, 10, 00, 00, 000)  | new DateTime(2005, 03, 25, 11, 00, 00, 000)
        "2005-03-25 10:20/2005-03-25 10:21"               | "day" | new DateTime(2005, 03, 25, 10, 20, 00, 000)  | new DateTime(2005, 03, 25, 10, 21, 00, 000)
        "2005-03-25 10:20:30/2005-03-25 10:20:31"         | "day" | new DateTime(2005, 03, 25, 10, 20, 30, 000)  | new DateTime(2005, 03, 25, 10, 20, 31, 000)
        "2005-03-25 10:20:30.555/2005-03-25 10:20:30.556" | "day" | new DateTime(2005, 03, 25, 10, 20, 30, 555)  | new DateTime(2005, 03, 25, 10, 20, 30, 556)

        "2005-03-25T10/2005-03-25 11"                     | "day" | new DateTime(2005, 03, 25, 10, 00, 00, 000)  | new DateTime(2005, 03, 25, 11, 00, 00, 000)
        "2005-03-25T10:20/2005-03-25 10:21"               | "day" | new DateTime(2005, 03, 25, 10, 20, 00, 000)  | new DateTime(2005, 03, 25, 10, 21, 00, 000)
        "2005-03-25 10:20:30/2005-03-25T10:20:31"         | "day" | new DateTime(2005, 03, 25, 10, 20, 30, 000)  | new DateTime(2005, 03, 25, 10, 20, 31, 000)
        "2005-03-25 10:20:30.555/2005-03-25T10:20:30.556" | "day" | new DateTime(2005, 03, 25, 10, 20, 30, 555)  | new DateTime(2005, 03, 25, 10, 20, 30, 556)

        "2005/2006-04"                                    | "day" | new DateTime(2005, 01, 01, 00, 00, 00, 000)  | new DateTime(2006, 04, 01, 00, 00, 00, 000)
        "2005-03/2005-03-26"                              | "day" | new DateTime(2005, 03, 01, 00, 00, 00, 000)  | new DateTime(2005, 03, 26, 00, 00, 00, 000)
        "2005-03-25/2005-03-25T11"                        | "day" | new DateTime(2005, 03, 25, 00, 00, 00, 000)  | new DateTime(2005, 03, 25, 11, 00, 00, 000)
        "2005-03-25T10/2005-03-25T10:21"                  | "day" | new DateTime(2005, 03, 25, 10, 00, 00, 000)  | new DateTime(2005, 03, 25, 10, 21, 00, 000)
        "2005-03-25T10:20/2005-03-25T10:20:31"            | "day" | new DateTime(2005, 03, 25, 10, 20, 00, 000)  | new DateTime(2005, 03, 25, 10, 20, 31, 000)
        "2005-03-25T10:20:30/2005-03-25T10:20:30.556"     | "day" | new DateTime(2005, 03, 25, 10, 20, 30, 000)  | new DateTime(2005, 03, 25, 10, 20, 30, 556)

    }

    @Unroll
    def "check parsing interval #intervalString parses to #parsedStart/#parsedStop with the use of time periods"() {

        given: "An expected interval"
        Interval expectedInterval = new Interval(parsedStart, parsedStop)

        expect: "The interval string parses into a single interval"
        new TestingDataApiRequestImpl().generateIntervals(
                intervalString,
                apiRequest.generateGranularity(name, granularityParser), dateTimeFormatter
        ).size() == 1

        and: "It parses to the interval we expect"
        new TestingDataApiRequestImpl().generateIntervals(
                intervalString,
                apiRequest.generateGranularity(name, granularityParser), dateTimeFormatter
        ).first() == expectedInterval

        where:
        intervalString                     | name      | parsedStart                                    | parsedStop
        "2005-03-25T10:20:30/P3D"          | "day"     | new DateTime(2005, 03, 25, 10, 20, 30, 000)    | new DateTime(2005, 03, 28, 10, 20, 30, 000)
        "P3D/2005-03-25T10:20:30"          | "day"     | new DateTime(2005, 03, 22, 10, 20, 30, 000)    | new DateTime(2005, 03, 25, 10, 20, 30, 000)
        "P1Y2M3D/2005-03-25T10:20:30"      | "day"     | new DateTime(2004, 01, 22, 10, 20, 30, 000)    | new DateTime(2005, 03, 25, 10, 20, 30, 000)
        "2005-03-25T00:00:00/P1Y2M3D"      | "day"     | new DateTime(2005, 03, 25, 00, 00, 00, 000)    | new DateTime(2006, 05, 28, 00, 00, 00, 000)
        "2015-10-19T00:00:00/P2W"          | "day"     | new DateTime(2015, 10, 19, 00, 00, 00, 000)    | new DateTime(2015, 11, 02, 00, 00, 00, 000)

        "2005-03-25T10:20:30/P3D"          | "all"     | new DateTime(2005, 03, 25, 10, 20, 30, 000)    | new DateTime(2005, 03, 28, 10, 20, 30, 000)
        "P3D/2005-03-25T10:20:30"          | "all"     | new DateTime(2005, 03, 22, 10, 20, 30, 000)    | new DateTime(2005, 03, 25, 10, 20, 30, 000)
        "P1Y2M3D/2005-03-25T10:20:30"      | "all"     | new DateTime(2004, 01, 22, 10, 20, 30, 000)    | new DateTime(2005, 03, 25, 10, 20, 30, 000)
        "2005-03-25T00:00:00/P1Y2M3D"      | "all"     | new DateTime(2005, 03, 25, 00, 00, 00, 000)    | new DateTime(2006, 05, 28, 00, 00, 00, 000)
        "2015-10-19T00:00:00/P2W"          | "all"     | new DateTime(2015, 10, 19, 00, 00, 00, 000)    | new DateTime(2015, 11, 02, 00, 00, 00, 000)

        "2015-10-19T00:00:00/P3D"          | "day"     | new DateTime(2015, 10, 19, 00, 00, 00, 000)    | new DateTime(2015, 10, 22, 00, 00, 00, 000)
        "P3D/2015-10-19T00:00:00"          | "day"     | new DateTime(2015, 10, 16, 00, 00, 00, 000)    | new DateTime(2015, 10, 19, 00, 00, 00, 000)
        "P1Y2M3D/2005-03-25T10:20:30"      | "day"     | new DateTime(2004, 01, 22, 10, 20, 30, 000)    | new DateTime(2005, 03, 25, 10, 20, 30, 000)
        "2005-03-25T00:00:00/P1Y2M3D"      | "day"     | new DateTime(2005, 03, 25, 00, 00, 00, 000)    | new DateTime(2006, 05, 28, 00, 00, 00, 000)

        "2015-10-01T00:00:00/P3M"          | "month"   | new DateTime(2015, 10, 01, 00, 00, 00, 000)    | new DateTime(2016, 01, 01, 00, 00, 00, 000)
        "P3M/2015-10-01T00:00:00"          | "month"   | new DateTime(2015, 07, 01, 00, 00, 00, 000)    | new DateTime(2015, 10, 01, 00, 00, 00, 000)
        "P1Y2M/2005-03-01T10:20:30"        | "month"   | new DateTime(2004, 01, 01, 10, 20, 30, 000)    | new DateTime(2005, 03, 01, 10, 20, 30, 000)
        "2005-03-01T00:00:00/P1Y2M"        | "month"   | new DateTime(2005, 03, 01, 00, 00, 00, 000)    | new DateTime(2006, 05, 01, 00, 00, 00, 000)

        "2015-01-01T00:00:00/P3Y"          | "year"    | new DateTime(2015, 01, 01, 00, 00, 00, 000)    | new DateTime(2018, 01, 01, 00, 00, 00, 000)
        "P3Y/2015-01-01T00:00:00"          | "year"    | new DateTime(2012, 01, 01, 00, 00, 00, 000)    | new DateTime(2015, 01, 01, 00, 00, 00, 000)

        "2015-10-01T00:00:00/P3M"          | "quarter" | new DateTime(2015, 10, 01, 00, 00, 00, 000)    | new DateTime(2016, 01, 01, 00, 00, 00, 000)
        "P3M/2015-10-01T00:00:00"          | "quarter" | new DateTime(2015, 07, 01, 00, 00, 00, 000)    | new DateTime(2015, 10, 01, 00, 00, 00, 000)

        "2015-10-19T00:00:00/P2W"          | "week"    | new DateTime(2015, 10, 19, 00, 00, 00, 000)    | new DateTime(2015, 11, 02, 00, 00, 00, 000)
        "P2W/2015-10-19T00:00:00"          | "week"    | new DateTime(2015, 10, 05, 00, 00, 00, 000)    | new DateTime(2015, 10, 19, 00, 00, 00, 000)
    }

    @Unroll
    def "check parsing interval #intervalString parses to #parsedStart/#parsedStop with the use of time periods and macros"() {

        given: "An expected interval"
        Interval expectedInterval = new Interval(parsedStart, parsedStop)

        expect: "The interval string parses into a single interval"
        new TestingDataApiRequestImpl().generateIntervals(
                intervalString,
                apiRequest.generateGranularity(name, granularityParser),
                dateTimeFormatter
        ).size() == 1

        and: "It parses to the interval we expect"
        new TestingDataApiRequestImpl().generateIntervals(
                intervalString,
                apiRequest.generateGranularity(name, granularityParser),
                dateTimeFormatter
        ).first() == expectedInterval

        where:
        intervalString                  | name      | parsedStart                                                                 | parsedStop
        "P3Y/current"                   | "year"    | dateParser.apply(YEAR).minusYears(3)     | dateParser.apply(YEAR)
        "current/P3Y"                   | "year"    | dateParser.apply(YEAR)                  | dateParser.apply(YEAR).plusYears(3)
        "P3Y/next"                      | "year"    | dateParser.apply(YEAR).minusYears(2)     | dateParser.apply(YEAR).plusYears(1)
        "current/next"                  | "year"    | dateParser.apply(YEAR)                   | dateParser.apply(YEAR).plusYears(1)

        "P3M/current"                   | "quarter" | dateParser.apply(QUARTER).minusMonths(3) | dateParser.apply(QUARTER)
        "P3M/next"                      | "quarter" | dateParser.apply(QUARTER)                | dateParser.apply(QUARTER).plusMonths(3)
        "current/P3M"                   | "quarter" | dateParser.apply(QUARTER)                | dateParser.apply(QUARTER).plusMonths(3)
        "current/next"                  | "quarter" | dateParser.apply(QUARTER)               | dateParser.apply(QUARTER).plusMonths(3)

        "P2W/current"                   | "week"    | dateParser.apply(WEEK).minusWeeks(2)     | dateParser.apply(WEEK)
        "current/P2W"                   | "week"    | dateParser.apply(WEEK)                  | dateParser.apply(WEEK).plusWeeks(2)
        "P2W/next"                      | "week"    | dateParser.apply(WEEK).minusWeeks(1)     | dateParser.apply(WEEK).plusWeeks(1)
        "current/next"                  | "week"    | dateParser.apply(WEEK)                  | dateParser.apply(WEEK).plusWeeks(1)

        "P3D/current"                   | "day"     | dateParser.apply(DAY).minusDays(3)       | dateParser.apply(DAY)
        "current/P3D"                   | "day"     | dateParser.apply(DAY)                   | dateParser.apply(DAY).plusDays(3)
        "P3D/next"                      | "day"     | dateParser.apply(DAY).minusDays(2)       | dateParser.apply(DAY).plusDays(1)
        "current/next"                  | "day"     | dateParser.apply(DAY)                    | dateParser.apply(DAY).plusDays(1)

        "P3M/current"                   | "month"   | dateParser.apply(MONTH).minusMonths(3)   | dateParser.apply(MONTH)
        "current/P3M"                   | "month"   | dateParser.apply(MONTH)                 | dateParser.apply(MONTH).plusMonths(3)
        "P3M/next"                      | "month"   | dateParser.apply(MONTH).minusMonths(2)   | dateParser.apply(MONTH).plusMonths(1)
        "current/next"                  | "month"   | dateParser.apply(MONTH)                 | dateParser.apply(MONTH).plusMonths(1)
    }

    @Unroll
    def "check invalid usage of macros as time intervals string #intervalString"() {
        when:
        new TestingDataApiRequestImpl().generateIntervals(
                intervalString,
                apiRequest.generateGranularity(name, granularityParser),
                dateTimeFormatter
        )

        then:
        thrown reason

        where:
        intervalString    | name  | reason
        "P3D/P3D"         | "day" | BadApiRequestException
        "next/next"       | "day" | BadApiRequestException
        "current/current" | "day" | BadApiRequestException
        "current/P3D"     | "all" | BadApiRequestException
        "P3D/next"        | "all" | BadApiRequestException
    }

    @Unroll
    def "check parsing multiple intervals"() {

        HashSet<Interval> expected =
        [interval1, interval2].collect { String value ->
            new Interval(value)
        }

        expect:
        Set<Interval> intervals = new TestingDataApiRequestImpl().generateIntervals(
                interval1 + "," + interval2,
                apiRequest.generateGranularity("day", granularityParser),
                dateTimeFormatter
        )
        intervals == expected

        where:
        interval1                                 | interval2
        "2005-03-25T10:20:30/2005-03-26T10:20:30" | "2005-04-25T10:20:30/2005-04-26T10:20:30"
    }

    @Unroll
    def "check bad generateIntervals throws #reason.simpleName"() {
        when:
        new TestingDataApiRequestImpl().generateIntervals(
                interval1 + "," + interval2,
                apiRequest.generateGranularity("day", granularityParser),
                dateTimeFormatter
        )

        then:
        thrown reason

        where:
        interval1                                 | interval2                                 | reason
        "2005-02-30T10:20:30/2005-03-26T10:20:30" | "2005-04-25T10:20:30/2005-04-26T10:20:30" | BadApiRequestException
        // incorrect date 02-30
    }
}
