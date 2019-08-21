// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.QUARTER
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.QUARTER
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.QUARTER
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.QUARTER
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.QUARTER
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.QUARTER
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.QUARTER
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.QUARTER
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.util.DateTimeFormatterFactory.FULLY_OPTIONAL_DATETIME_FORMATTER
import static com.yahoo.bard.webservice.util.DateTimeFormatterFactory.FULLY_OPTIONAL_DATETIME_FORMATTER
import static com.yahoo.bard.webservice.util.DateTimeFormatterFactory.FULLY_OPTIONAL_DATETIME_FORMATTER
import static com.yahoo.bard.webservice.util.DateTimeFormatterFactory.FULLY_OPTIONAL_DATETIME_FORMATTER
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TIME_ALIGNMENT

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.data.time.GranularityParser
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.data.time.TimeGrain
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.util.DateTimeFormatterFactory
import com.yahoo.bard.webservice.util.IntervalUtils
import com.yahoo.bard.webservice.web.BadApiRequestException
import com.yahoo.bard.webservice.web.apirequest.utils.TestingDataApiRequestImpl

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.joda.time.format.DateTimeFormatter

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.function.Function

class IntervalGenerationUtilsSpec extends Specification {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance()

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
            metricDict.put(name, new LogicalMetric(null, null, name))
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
    def "Test adjustment of server time when #description"() {

        when:

        SYSTEM_CONFIG.setProperty("bard__adjusted_time_zone",zone)
        DateTime adjustedNow = IntervalGenerationUtils.getAdjustedTime(new DateTime(serverTime))

        then:
        adjustedNow.toString() == expectedDateTime

        where:
        serverTime                          |   expectedDateTime                    | zone                  | description
        "2019-04-24T00:13:00.564Z"          |   "2019-04-23T17:13:00.564Z"          | "America/Los_Angeles" | "server time is past midnight UTC and timezone to adjust is PST"
        "2019-04-23T19:13:00.564Z"          |   "2019-04-23T19:13:00.564Z"          | "UTC"                 | "server time is in UTC and timezone to adjust to is also UTC"
        "2019-04-23T10:13:00.564Z"          |   "2019-04-23T03:13:00.564Z"          | "America/Los_Angeles" | "server time is before midnight UTC and timezone to adjust is PST"
    }

    @Unroll
    def "check parsing interval #intervalString parses to #parsedStart/#parsedStop"() {

        given: "An expected interval"
        Interval expectedInterval = new Interval(parsedStart, parsedStop)

        expect: "The interval string parses into a single interval"
        IntervalGenerationUtils.generateIntervals(
                intervalString,
                GranularityGenerator.DEFAULT_GRANULARITY_GENERATOR.generateGranularity(name, granularityParser),
                dateTimeFormatter
        ).size() == 1

        and: "It parses to the interval we expect"
        IntervalGenerationUtils.generateIntervals(
                intervalString,
                GranularityGenerator.DEFAULT_GRANULARITY_GENERATOR.generateGranularity(name, granularityParser),
                dateTimeFormatter
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
        IntervalGenerationUtils.generateIntervals(
                intervalString,
                GranularityGenerator.DEFAULT_GRANULARITY_GENERATOR.generateGranularity(name, granularityParser),
                dateTimeFormatter
        ).size() == 1

        and: "It parses to the interval we expect"
        IntervalGenerationUtils.generateIntervals(
                intervalString,
                GranularityGenerator.DEFAULT_GRANULARITY_GENERATOR.generateGranularity(name, granularityParser),
                dateTimeFormatter
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
        IntervalGenerationUtils.generateIntervals(
                intervalString,
                GranularityGenerator.DEFAULT_GRANULARITY_GENERATOR.generateGranularity(name, granularityParser),
                dateTimeFormatter
        ).size() == 1

        and: "It parses to the interval we expect"
        IntervalGenerationUtils.generateIntervals(
                intervalString,
                GranularityGenerator.DEFAULT_GRANULARITY_GENERATOR.generateGranularity(name, granularityParser),
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
        IntervalGenerationUtils.generateIntervals(
                intervalString,
                GranularityGenerator.DEFAULT_GRANULARITY_GENERATOR.generateGranularity(name, granularityParser),
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
        Set<Interval> intervals = IntervalGenerationUtils.generateIntervals(
                interval1 + "," + interval2,
                GranularityGenerator.DEFAULT_GRANULARITY_GENERATOR.generateGranularity("day", granularityParser),
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
        IntervalGenerationUtils.generateIntervals(
                interval1 + "," + interval2,
                GranularityGenerator.DEFAULT_GRANULARITY_GENERATOR.generateGranularity("day", granularityParser),
                dateTimeFormatter
        )

        then:
        thrown reason

        where:
        interval1                                 | interval2                                 | reason
        "2005-02-30T10:20:30/2005-03-26T10:20:30" | "2005-04-25T10:20:30/2005-04-26T10:20:30" | BadApiRequestException
        // incorrect date 02-30
    }

    @Unroll
    def "Test validate daily alignment with #intervalString and #zone"() {
        DateTimeFormatter dateTimeFormatter = FULLY_OPTIONAL_DATETIME_FORMATTER

        DateTimeZone dateTimeZone = DateTimeZone.forID(zone)
        List<Interval> intervals = IntervalGenerationUtils.generateIntervals(
                intervalString,
                DAY,
                dateTimeFormatter.withZone(dateTimeZone)
        )

        expect:
        IntervalGenerationUtils.validateTimeAlignment(DAY, intervals)

        where:
        intervalString          | zone
        "2005-03-25/2005-03-26" | "UTC"
        "2005-03-25/2005-03-26" | "US/Pacific"
    }

    def "check invalid week granularity alignment creates error"() {
        setup:
        String expectedMessage = "'[2015-02-15T00:00:00.000Z/2016-02-22T00:00:00.000Z]'"
        expectedMessage += " does not align with granularity 'week'."
        expectedMessage += " Week must start on a Monday and end on a Monday."
        Granularity granularity = GranularityGenerator.DEFAULT_GRANULARITY_GENERATOR.generateGranularity(
                "week",
                new StandardGranularityParser()
        )
        Set<Interval> intervals = IntervalGenerationUtils.generateIntervals(
                "2015-02-15/2016-02-22",
                granularity,
                FULLY_OPTIONAL_DATETIME_FORMATTER
        )

        expect:
        !granularity.accepts(intervals)
        TIME_ALIGNMENT.logFormat(intervals, granularity, granularity.getAlignmentDescription()) == expectedMessage
    }

    @Unroll
    def "Time parsed with #timeZone vs UTC is #hours apart"() {
        setup:
        String baseTimeString = "2015-01-01T15:00";
        DateTime baseTime = DateTime.parse(baseTimeString, FULLY_OPTIONAL_DATETIME_FORMATTER);
        DateTimeZone dateTimeZone = DateTimeZone.forID(timeZone)

        DateTimeFormatter adjustedFormatter = FULLY_OPTIONAL_DATETIME_FORMATTER.withZone(dateTimeZone)

        expect:
        TestingDataApiRequestImpl.getAsDateTime(
                null,
                null,
                baseTimeString,
                adjustedFormatter
        ).plusHours(hours) == baseTime

        where:

        hours | timeZone
        0     | "UTC"
        -10   | "Pacific/Honolulu"
        -8    | "US/Pacific"
        8     | "Asia/Hong_Kong"
    }
}
