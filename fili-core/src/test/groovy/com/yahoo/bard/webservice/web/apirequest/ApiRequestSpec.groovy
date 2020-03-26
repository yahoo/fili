// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.util.DateTimeFormatterFactory.FULLY_OPTIONAL_DATETIME_FORMATTER
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TIME_ALIGNMENT

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.AllGranularity
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.data.time.GranularityParser
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.util.IntervalUtils
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.DefaultResponseFormatType
import com.yahoo.bard.webservice.web.ErrorMessageFormat
import com.yahoo.bard.webservice.web.apirequest.utils.TestingDataApiRequestImpl

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.joda.time.format.DateTimeFormatter

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.PathSegment

class ApiRequestSpec extends Specification {

    @Shared
    DimensionDictionary dimensionDict
    @Shared
    MetricDictionary metricDict
    @Shared
    LogicalTable table

    GranularityParser granularityParser = new StandardGranularityParser()

    def setupSpec() {
        DateTimeZone.default = IntervalUtils.SYSTEM_ALIGNMENT_EPOCH.zone
    }

    def setup() {
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        dimensionDict = new DimensionDictionary()
        KeyValueStoreDimension keyValueStoreDimension
        [ "locale", "one", "two", "three" ].each { String name ->
            keyValueStoreDimension = new KeyValueStoreDimension(
                    name,
                    "desc" + name,
                    dimensionFields,
                    MapStoreManager.getInstance(name),
                    ScanSearchProviderManager.getInstance(name)
            )
            keyValueStoreDimension.setLastUpdated(new DateTime(10000))
            dimensionDict.add(keyValueStoreDimension)
        }

        metricDict = new MetricDictionary()
        [ "met1", "met2", "met3", "met4" ].each { String name ->
            metricDict.put(name, new LogicalMetricImpl(null, null, name))
        }
        TableGroup tg = Mock(TableGroup)
        tg.getApiMetricNames() >> ([] as Set)
        tg.getDimensions() >> dimensionDict.apiNameToDimension.values()
        table = new LogicalTable("name", DAY, tg, metricDict)
    }

    def "check valid parsing generateFormat"() {

        expect:
        responseFormat == expectedFormat

        where:
        responseFormat                 | expectedFormat
        DefaultResponseFormatType.JSON | new TestingDataApiRequestImpl().generateAcceptFormat(null)
        DefaultResponseFormatType.JSON | new TestingDataApiRequestImpl().generateAcceptFormat("json")
        DefaultResponseFormatType.CSV  | new TestingDataApiRequestImpl().generateAcceptFormat("csv")
    }

    def "check invalid parsing generateFormat"() {
        when:
        new TestingDataApiRequestImpl().generateAcceptFormat("bad")

        then:
        thrown BadApiRequestException
    }

    @Unroll
    def "check valid granularity name #name parses to granularity #expected"() {
        expect:
        new TestingDataApiRequestImpl().generateGranularity(name, granularityParser) == expected

        where:
        name  | expected
        "day" | DAY
        "all" | AllGranularity.INSTANCE
    }

    def "check invalid granularity creates error"() {
        setup: "Define an improper granularity name"
        String timeGrainName = "seldom"
        String expectedMessage = ErrorMessageFormat.UNKNOWN_GRANULARITY.format(timeGrainName)

        when:
        new TestingDataApiRequestImpl().generateGranularity(timeGrainName, new StandardGranularityParser())

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == expectedMessage
    }

    def "check empty generateDimensions"() {

        Set<Dimension> dims = new TestingDataApiRequestImpl().generateDimensions(
                new ArrayList<PathSegment>(),
                dimensionDict
        )

        expect:
        dims == [] as Set
    }

    def "check parsing generateDimensions"() {

        PathSegment one = Mock(PathSegment)
        PathSegment two = Mock(PathSegment)
        PathSegment three = Mock(PathSegment)
        Map emptyMap = new MultivaluedHashMap<>()

        one.getPath() >> "one"
        one.getMatrixParameters() >> emptyMap
        two.getPath() >> "two"
        two.getMatrixParameters() >> emptyMap
        three.getPath() >> "three"
        three.getMatrixParameters() >> emptyMap

        Set<Dimension> dims = new TestingDataApiRequestImpl().generateDimensions([one, two, three], dimensionDict)

        HashSet<Dimension> expected =
                ["one", "two", "three"].collect { String name ->
                    Dimension dim = dimensionDict.findByApiName(name)
                    assert dim?.apiName == name
                    dim
                }

        expect:
        dims == expected
    }

    @Unroll
    def "Test validate daily alignment with #intervalString and #zone"() {
        DateTimeFormatter dateTimeFormatter = FULLY_OPTIONAL_DATETIME_FORMATTER

        DateTimeZone dateTimeZone = DateTimeZone.forID(zone)
        List<Interval> intervals = TestingDataApiRequestImpl.generateIntervals(
                intervalString,
                DAY,
                dateTimeFormatter.withZone(dateTimeZone)
        )

        expect:
        TestingDataApiRequestImpl.validateTimeAlignment(DAY, intervals)

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
        Granularity granularity = new TestingDataApiRequestImpl().generateGranularity(
                "week",
                new StandardGranularityParser()
        )
        Set<Interval> intervals = new TestingDataApiRequestImpl().generateIntervals(
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
                adjustedFormatter).plusHours(hours) == baseTime

        where:

        hours | timeZone
        0     | "UTC"
        -10   | "Pacific/Honolulu"
        -8    | "US/Pacific"
        8     | "Asia/Hong_Kong"
    }
}
