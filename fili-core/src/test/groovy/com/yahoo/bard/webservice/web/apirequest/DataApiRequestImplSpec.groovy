// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.config.BardFeatureFlag
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.dimension.impl.SimpleVirtualDimension
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.AllGranularity
import com.yahoo.bard.webservice.data.time.GranularityParser
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.druid.model.builders.DruidInFilterBuilder
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.util.IntervalUtils
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.DefaultResponseFormatType
import com.yahoo.bard.webservice.web.ErrorMessageFormat
import com.yahoo.bard.webservice.web.FilteredThetaSketchMetricsHelper
import com.yahoo.bard.webservice.web.MetricsFilterSetBuilder
import com.yahoo.bard.webservice.web.apirequest.utils.TestPathSegment
import com.yahoo.bard.webservice.web.apirequest.utils.TestingDataApiRequestImpl

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.PathSegment

class DataApiRequestImplSpec extends Specification {

    @Shared
    DimensionDictionary dimensionDict
    @Shared
    MetricDictionary metricDict
    @Shared
    LogicalTable table

    GranularityParser granularityParser = new StandardGranularityParser()

    public static final DataApiRequestImpl REQUEST = TestingDataApiRequestImpl.buildStableDataApiRequestImpl()

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
            metricDict.put(name, new LogicalMetricImpl(null, null, name))
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
        DefaultResponseFormatType.JSON | REQUEST.generateAcceptFormat(null)
        DefaultResponseFormatType.JSON | REQUEST.generateAcceptFormat("json")
        DefaultResponseFormatType.CSV  | REQUEST.generateAcceptFormat("csv")
    }

    def "check invalid parsing generateFormat"() {
        when:
        REQUEST.generateAcceptFormat("bad")

        then:
        thrown BadApiRequestException
    }

    def "check parsing generateLogicalMetrics"() {
        given:
        Set<LogicalMetric> logicalMetrics = REQUEST.generateLogicalMetrics(
                "met1,met2,met3",
                table,
                metricDict,
                dimensionDict
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


    def "generateDimensions parses known dimensions"() {
        when:
        List<PathSegment> pathSegmentList = new ArrayList<>()

        pathSegmentList.add(new TestPathSegment("locale", null))
        pathSegmentList.add(new TestPathSegment("one", "desc"))

        Set<Dimension> groupDimensions = REQUEST.generateDimensions(
                pathSegmentList,
                dimensionDict
        )

        then:
        groupDimensions.size() == 2
    }

    def "generatePerDimensionFields parses known dimensions"() {
        when:
        List<PathSegment> pathSegmentList = new ArrayList<>()

        pathSegmentList.add(new TestPathSegment("locale", null))
        pathSegmentList.add(new TestPathSegment("one", "desc"))

        Dimension locale = dimensionDict.findByApiName("locale")
        Dimension one = dimensionDict.findByApiName("one")

        Set<Dimension> dimensions = [locale, one] as LinkedHashSet
        LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> dimensionFields =
                REQUEST.generateDimensionFields(pathSegmentList, dimensions)

        then:
        dimensionFields.keySet().size() == 2
        dimensionFields.get(locale).size() == 2
        dimensionFields.get(one).size() == 1
        dimensionFields.get(one).first().name == "desc"
    }

    def "generatePerDimensionFields ignores unknown dimensions"() {
        when:
        List<PathSegment> pathSegmentList = new ArrayList<>()

        pathSegmentList.add(new TestPathSegment("locale", null))
        pathSegmentList.add(new TestPathSegment("one", "desc"))
        pathSegmentList.add(new TestPathSegment("__unconfigured", null))

        Dimension locale = dimensionDict.findByApiName("locale")
        Dimension one = dimensionDict.findByApiName("one")
        Dimension unconfigured = new SimpleVirtualDimension("__unconfigured")

        Set<Dimension> dimensions = [locale, one] as LinkedHashSet


        LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> dimensionFields =
                REQUEST.generateDimensionFields(pathSegmentList, dimensions)

        then:
        dimensionFields.keySet().size() == 2
        dimensionFields.get(locale).size() == 2
        dimensionFields.get(one).size() == 1
        dimensionFields.get(one).first().name == "desc"
    }

    def "generateDimensions fails on unknown dimensions"() {
        when:
        List<PathSegment> pathSegmentList = new ArrayList<>()

        pathSegmentList.add(new TestPathSegment("locale", null))
        pathSegmentList.add(new TestPathSegment("__unconfigured", null))

        Set<Dimension> groupDimensions = REQUEST.generateDimensions(
                pathSegmentList,
                dimensionDict
        )

        then:
        thrown(BadApiRequestException)
    }

    def "generateLogicalMetrics throws BadApiRequestException on non-existing LogicalMetric"() {
        when:
        Set<LogicalMetric> logicalMetrics = REQUEST.generateLogicalMetrics(
                "met1,met2,nonExistingMetric",
                table,
                metricDict,
                dimensionDict
        )

        then: "BadApiRequestException is thrown"
        BadApiRequestException exception = thrown()
        exception.message == ErrorMessageFormat.METRICS_UNDEFINED.logFormat(["nonExistingMetric"])
    }

    @Unroll
    def "check valid granularity name #name parses to granularity #expected"() {
        expect:
        REQUEST.generateGranularity(name, granularityParser) == expected

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
        REQUEST.generateGranularity(timeGrainName, granularityParser)

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == expectedMessage
    }

    def "if metrics are required an exception is thrown on request with no metrics"() {
        setup:
        BardFeatureFlag.REQUIRE_METRICS_QUERY.setOn(true)

        // set class to handle static provider method
        MetricsFilterSetBuilder backupFilterSetBuilder = FieldConverterSupplier.metricsFilterSetBuilder
        FieldConverterSupplier.metricsFilterSetBuilder = new FilteredThetaSketchMetricsHelper(new DruidInFilterBuilder())

        when:
        REQUEST.generateLogicalMetrics(
                "",
                table,
                metricDict,
                dimensionDict
        )

        then:
        thrown(BadApiRequestException)

        cleanup:
        BardFeatureFlag.REQUIRE_METRICS_QUERY.reset()
        FieldConverterSupplier.metricsFilterSetBuilder = backupFilterSetBuilder
    }

    @Unroll
    def "If metrics are NOT required and intersection reporting is #interreport then no error is thrown on request with no metrics"() {
        setup:
        // backup static system config
        BardFeatureFlag.INTERSECTION_REPORTING.setOn(intersectionReportingOn)
        BardFeatureFlag.REQUIRE_METRICS_QUERY.setOn(false)

        // set class to handle static provider method
        MetricsFilterSetBuilder backupFilterSetBuilder = FieldConverterSupplier.metricsFilterSetBuilder
        FieldConverterSupplier.metricsFilterSetBuilder = new FilteredThetaSketchMetricsHelper(new DruidInFilterBuilder())

        when:
        REQUEST.generateLogicalMetrics(
                "",
                table,
                metricDict,
                dimensionDict
        )

        then:
        noExceptionThrown()

        cleanup:
        BardFeatureFlag.INTERSECTION_REPORTING.reset()
        BardFeatureFlag.REQUIRE_METRICS_QUERY.reset()
        FieldConverterSupplier.metricsFilterSetBuilder = backupFilterSetBuilder

        where:
        intersectionReportingOn | interreport
        false                   | "disabled"
        true                    | "enabled"
    }
}
