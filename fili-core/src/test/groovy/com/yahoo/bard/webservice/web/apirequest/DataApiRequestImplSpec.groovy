// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.GranularityParser
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.data.time.AllGranularity
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.util.IntervalUtils
import com.yahoo.bard.webservice.web.BadApiRequestException
import com.yahoo.bard.webservice.web.ErrorMessageFormat
import com.yahoo.bard.webservice.web.DefaultResponseFormatType
import com.yahoo.bard.webservice.web.apirequest.utils.TestingDataApiRequestImpl

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class DataApiRequestImplSpec extends Specification {

    @Shared
    DimensionDictionary dimensionDict
    @Shared
    MetricDictionary metricDict
    @Shared
    LogicalTable table

    GranularityParser granularityParser = new StandardGranularityParser()

    static final DateTimeZone orginalTimeZone = DateTimeZone.default

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
        tg.getApiMetricNames() >> ([] as Set)
        tg.getDimensions() >> dimensionDict.apiNameToDimension.values()
        table = new LogicalTable("name", DAY, tg, metricDict)
    }

    def cleanupSpec() {
        DateTimeZone.default = orginalTimeZone
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

    def "check parsing generateLogicalMetrics"() {
        given:
        Set<LogicalMetric> logicalMetrics = new TestingDataApiRequestImpl().generateLogicalMetrics(
                "met1,met2,met3",
                metricDict,
                dimensionDict,
                table
        )

        HashSet<Dimension> expected =
        ["met1", "met2", "met3" ].collect { String name ->
            LogicalMetric metric = metricDict.get(name)
            assert metric?.name == name
            metric
        }

        expect:
        logicalMetrics == expected
    }

    def "generateLogicalMetrics throws BadApiRequestException on non-existing LogicalMetric"() {
        when:
        Set<LogicalMetric> logicalMetrics = new TestingDataApiRequestImpl().generateLogicalMetrics(
                "met1,met2,nonExistingMetric",
                metricDict,
                dimensionDict,
                table
        )

        then: "BadApiRequestException is thrown"
        BadApiRequestException exception = thrown()
        exception.message == ErrorMessageFormat.METRICS_UNDEFINED.logFormat(["nonExistingMetric"])
    }

    @Unroll
    def "check valid granularity name #name parses to granularity #expected"() {
        expect:
        new TestingDataApiRequestImpl().generateGranularity(name, granularityParser) == expected

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
        new TestingDataApiRequestImpl().generateGranularity(timeGrainName, granularityParser)

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == expectedMessage
    }

    def "check weekly granularity has the expected alignment description"() {
        setup:
        String expectedMessage = " Week must start on a Monday and end on a Monday."
        Granularity<?> granularity = new TestingDataApiRequestImpl().generateGranularity(
                "week",
                new StandardGranularityParser()
        )

        expect:
        granularity.getAlignmentDescription() == expectedMessage
    }
}
