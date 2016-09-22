// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.table.PhysicalTable

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.util.DateTimeFormatterFactory.FULLY_OPTIONAL_DATETIME_FORMATTER
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TIME_ALIGNMENT

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.GranularityParser
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.druid.model.query.Granularity
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.util.IntervalUtils

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.joda.time.format.DateTimeFormatter

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.PathSegment

class DataApiRequestSpec extends Specification {

    @Shared
    DimensionDictionary dimensionDict
    @Shared
    MetricDictionary metricDict
    @Shared
    LogicalTable table

    GranularityParser granularityParser = new StandardGranularityParser()

    static final DateTimeZone orginalTimeZone = DateTimeZone.default

    class ConcreteApiRequest extends ApiRequest {}
    ConcreteApiRequest concreteApiRequest = new ConcreteApiRequest()

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
            metricDict.put(name, new LogicalMetric(null, null, name))
        }
        TableGroup tg = Mock(TableGroup)
        tg.getDimensions() >> dimensionDict.apiNameToDimension.values()
        table = new LogicalTable("name", DAY, tg)
        dimensionDict.apiNameToDimension.values().each {
            DimensionColumn.addNewDimensionColumn(table, it, new PhysicalTable("abc", DAY.buildZonedTimeGrain(DateTimeZone.UTC), [:]))
        }
    }

    def cleanupSpec() {
        DateTimeZone.default = orginalTimeZone
    }

    def "check valid parsing generateFormat"() {

        expect:
        responseFormat == expectedFormat

        where:
        responseFormat          | expectedFormat
        ResponseFormatType.JSON | new DataApiRequest().generateAcceptFormat(null)
        ResponseFormatType.JSON | new DataApiRequest().generateAcceptFormat("json")
        ResponseFormatType.CSV  | new DataApiRequest().generateAcceptFormat("csv")
    }

    def "check invalid parsing generateFormat"() {
        when:
        new DataApiRequest().generateAcceptFormat("bad")

        then:
        thrown BadApiRequestException
    }


    def "check empty generateDimensions"() {

        Set<Dimension> dims = new DataApiRequest().generateDimensions(new ArrayList<PathSegment>(), dimensionDict)

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

        Set<Dimension> dims = new DataApiRequest().generateDimensions([one, two, three], dimensionDict)

        HashSet<Dimension> expected =
        ["one", "two", "three"].collect { String name ->
            Dimension dim = dimensionDict.findByApiName(name)
            assert dim?.apiName == name
            dim
        }

        expect:
        dims == expected
    }

    def "check parsing generateLogicalMetrics"() {

        Set<LogicalMetric> logicalMetrics = new DataApiRequest().generateLogicalMetrics("met1,met2,met3", metricDict, dimensionDict, table)

        HashSet<Dimension> expected =
        ["met1", "met2", "met3" ].collect { String name ->
            LogicalMetric metric = metricDict.get(name)
            assert metric?.name == name
            metric
        }
        expect:
        logicalMetrics == expected
    }

    @Unroll
    def "check valid granularity name #name parses to granularity #expected"() {
        expect:
        new DataApiRequest().generateGranularity(name, granularityParser) == expected

        where:
        name    | expected
        "day"   | DAY
        "all"   | AllGranularity.INSTANCE
    }

    def "check invalid granularity creates error"() {
        setup: "Define an improper granularity name"
        String timeGrainName = "seldom"
        String expectedMessage = ErrorMessageFormat.UNKNOWN_GRANULARITY.format(timeGrainName)

        when:
        new DataApiRequest().generateGranularity(timeGrainName, granularityParser)

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == expectedMessage
    }

    def "check weekly granularity has the expected alignment description"() {
        setup:
        String expectedMessage = " Week must start on a Monday and end on a Monday."
        Granularity<?> granularity = new DataApiRequest().generateGranularity("week", new StandardGranularityParser())

        expect:
        granularity.getAlignmentDescription() == expectedMessage
    }

    def "check invalid week granularity alignment creates error"() {
        setup:
        String expectedMessage = "'[2015-02-15T00:00:00.000Z/2016-02-22T00:00:00.000Z]'"
        expectedMessage += " does not align with granularity 'week'."
        expectedMessage += " Week must start on a Monday and end on a Monday."
        Granularity<?> granularity = new DataApiRequest().generateGranularity("week", new StandardGranularityParser())
        Set<Interval> intervals = new DataApiRequest().generateIntervals(
                "2015-02-15/2016-02-22",
                granularity,
                FULLY_OPTIONAL_DATETIME_FORMATTER
        )

        expect:
        granularity.accepts(intervals) == false
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
        DataApiRequest.getAsDateTime(null, null, baseTimeString, adjustedFormatter).plusHours(hours) == baseTime

        where:

        hours | timeZone
        0     | "UTC"
        -10   | "Pacific/Honolulu"
        -8    | "US/Pacific"
        8     | "Asia/Hong_Kong"
    }

    @Unroll
    def "Test validate daily alignment with #intervalString and #zone"() {
        DateTimeFormatter dateTimeFormatter = FULLY_OPTIONAL_DATETIME_FORMATTER

        DateTimeZone dateTimeZone = DateTimeZone.forID(zone)
        Set<Interval> intervals = DataApiRequest.generateIntervals(
                intervalString,
                DAY,
                dateTimeFormatter.withZone(dateTimeZone)
        )

        expect:
        DataApiRequest.validateTimeAlignment(DAY, intervals)

        where:
        intervalString          | zone
        "2005-03-25/2005-03-26" | "UTC"
        "2005-03-25/2005-03-26" | "US/Pacific"
    }
}
