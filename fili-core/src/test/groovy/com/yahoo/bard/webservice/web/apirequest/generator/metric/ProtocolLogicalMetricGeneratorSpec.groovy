// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.metric

import com.yahoo.bard.webservice.data.config.names.ApiMetricName
import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.TestApiMetricParser
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper
import com.yahoo.bard.webservice.data.metric.protocol.GeneratedMetricInfo
import com.yahoo.bard.webservice.data.metric.protocol.MetadataApplyTransformer
import com.yahoo.bard.webservice.data.metric.protocol.MetricTransformer
import com.yahoo.bard.webservice.data.metric.protocol.Protocol
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolDictionary
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolMetric
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolMetricImpl
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolSupport
import com.yahoo.bard.webservice.data.time.AllGranularity
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.table.TestPhysicalTable
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetric
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetricAnnotater

import spock.lang.Specification
import spock.lang.Unroll

class ProtocolLogicalMetricGeneratorSpec extends Specification {

    static ResultSetMapper TEST_RSM = new NoOpResultSetMapper()

    String testProtocolName
    Protocol testProtocol
    ProtocolDictionary protocolDictionary

    TemplateDruidQuery testTdq

    String baseMetadataTransformMetricName
    String rawMetadataTransformMetricName
    Aggregation baseMetadataAggregation
    ProtocolMetric baseMetric

    String validLogicalMetricName
    Aggregation validLogicalMetricAggregation
    LogicalMetric validLogicalMetric

    String invalidLogicalMetricName
    Aggregation invalidLogicalMetricAggregation
    LogicalMetric invalidLogicalMetric

    String validProtocolMetricBaseName
    Aggregation validProtocolMetricAggregation
    String validProtocolMetricRawName
    ProtocolMetric validProtocolMetricPreTransform
    ProtocolMetric validProtocolMetricPostTransform

    String invalidProtocolMetricBaseName
    Aggregation invalidProtocolMetricAggregation
    String invalidProtocolMetricRawName
    ProtocolMetric invalidProtocolMetricPreTransform
    ProtocolMetric invalidProtocolMetricPostTransform

    TableName testPhysicalTableName
    PhysicalTable testPhysicalTable

    String logicalTableName
    Granularity granularity
    TableGroup tableGroup
    LogicalTable logicalTable

    MetricDictionary metricDictionary

    ProtocolLogicalMetricGenerator generator


    def setup() {
        testProtocolName = "p"
        testProtocol = new Protocol(testProtocolName, new MetadataApplyTransformer())
        protocolDictionary = new ProtocolDictionary()
        protocolDictionary.add(testProtocol)

        generator = new ProtocolLogicalMetricGenerator(
                ApiMetricAnnotater.NO_OP_ANNOTATER,
                [testProtocolName],
                new TestApiMetricParser(),
                protocolDictionary,
        )

        // setup test metrics
        baseMetadataTransformMetricName = "baseMetric"
        rawMetadataTransformMetricName = "baseMetric(p=unused)"
        baseMetadataAggregation = new LongSumAggregation(baseMetadataTransformMetricName, "base_metric_field")

        validLogicalMetricName = "validLogicalMetric"
        validLogicalMetricAggregation = new LongSumAggregation(validLogicalMetricName, "valid_logical_metric_field")

        invalidLogicalMetricName = "invalidLogicalMetric"
        invalidLogicalMetricAggregation = new LongSumAggregation(
                invalidLogicalMetricName,
                "invalid_logical_metric_field"
        )

        validProtocolMetricBaseName = "validProtocolMetric"
        validProtocolMetricAggregation = new LongSumAggregation(
                validProtocolMetricBaseName,
                "valid_protocol_metric_field"
        )
        validProtocolMetricRawName = "RAW_validProtocolMetric"

        invalidProtocolMetricBaseName = "invalidProtocolMetric"
        invalidProtocolMetricAggregation = new LongSumAggregation(
                invalidProtocolMetricBaseName,
                "invalid_protocol_metric_field"
        )
        invalidProtocolMetricRawName = "RAW_invalidProtocolMetric"

        testTdq = new TemplateDruidQuery(
                [
                        baseMetadataAggregation,
                        validLogicalMetricAggregation,
                        invalidLogicalMetricAggregation,
                        validProtocolMetricAggregation,
                        invalidProtocolMetricAggregation
                ],
                []
        )
        baseMetric = new ProtocolMetricImpl(
                new LogicalMetricInfo(baseMetadataTransformMetricName),
                testTdq,
                TEST_RSM,
                new ProtocolSupport([testProtocol]),
        )
        metricDictionary = new MetricDictionary()
        metricDictionary.add(baseMetric)

        validLogicalMetric = new LogicalMetricImpl(
                new LogicalMetricInfo(validLogicalMetricName),
                testTdq,
                TEST_RSM
        )
        metricDictionary.add(validLogicalMetric)

        invalidLogicalMetric = new LogicalMetricImpl(
                new LogicalMetricInfo(invalidLogicalMetricName),
                testTdq,
                TEST_RSM
        )
        metricDictionary.add(invalidLogicalMetric)

        validProtocolMetricPreTransform = new ProtocolMetricImpl(
                new LogicalMetricInfo(validProtocolMetricBaseName),
                testTdq,
                TEST_RSM
        )
        metricDictionary.add(validProtocolMetricPreTransform)

        validProtocolMetricPostTransform = new ProtocolMetricImpl(
                new GeneratedMetricInfo(validProtocolMetricRawName, validProtocolMetricBaseName),
                testTdq.renameMetricField(validProtocolMetricBaseName, validProtocolMetricRawName),
                TEST_RSM
        )

        invalidProtocolMetricPreTransform = new ProtocolMetricImpl(
                new LogicalMetricInfo(invalidProtocolMetricBaseName),
                testTdq,
                TEST_RSM
        )
        metricDictionary.add(invalidProtocolMetricPreTransform)

        invalidProtocolMetricPostTransform = new ProtocolMetricImpl(
                new GeneratedMetricInfo(invalidProtocolMetricRawName, invalidProtocolMetricBaseName),
                testTdq.renameMetricField(invalidProtocolMetricBaseName, invalidProtocolMetricRawName),
                TEST_RSM
        )

        // Prepare test physical table
        testPhysicalTableName = TableName.of("testPhysicalTable")
        testPhysicalTable = new TestPhysicalTable.Builder(
                testPhysicalTableName,
                new DimensionDictionary()
        )
                .addMetricName(validLogicalMetricName)
                .addMetricName(validProtocolMetricBaseName)
                .build()

        // Prepare logical table
        logicalTableName = "testLogicalTable"
        granularity = DefaultTimeGrain.DAY
        tableGroup = new TableGroup(
                [testPhysicalTable] as LinkedHashSet,
                [ApiMetricName.of(validLogicalMetricName), ApiMetricName.of(validProtocolMetricBaseName)] as Set,
                [] as Set,
        )
        logicalTable = new LogicalTable(
                logicalTableName,
                granularity,
                tableGroup,
                metricDictionary
        )
    }

    boolean errorMessageContainsNames(String msg, Set<String> names) {
        String namesSlice = msg.split("'")[1]
        for (String name : names) {
            if (!namesSlice.contains(name)) {
                return false
            }
        }
        return true
    }

    def "Unresolvable metrics fail the query"() {
        setup:
        List<ApiMetric> missingMetrics = [
                new ApiMetric(
                        "missingMetric",
                        "missingMetric",
                        [:]
                )
        ]

        when:
        generator.applyProtocols(missingMetrics, metricDictionary)

        then:
        thrown(BadApiRequestException)
    }

    @Unroll
    def "getBaseName correctly sources base name from #infotype"() {
        expect:
        generator.getBaseName(info) == expected

        where:
        info                                              || expected     | infotype
        new LogicalMetricInfo("metricName")               || "metricName" | "Logical Metric Info"
        new GeneratedMetricInfo("metricName", "baseName") || "baseName"   | "Generated Metric Info"
    }

    // This test assumes the backing metric transformers follow the metadata contract
    def "Transformed metrics use properly formatted GeneratedMetricInfo"() {
        setup:
        List<ApiMetric> testApiMetrics = [new ApiMetric(
                rawMetadataTransformMetricName,
                baseMetadataTransformMetricName,
                [(testProtocolName): "unused"]
        )]

        when:
        LogicalMetric result = generator.applyProtocols(testApiMetrics, metricDictionary).iterator().next()
        GeneratedMetricInfo resultMetadata = result.getLogicalMetricInfo()

        then:
        result.getName() == rawMetadataTransformMetricName
        resultMetadata.getName() == rawMetadataTransformMetricName
        resultMetadata.getBaseMetricName() == baseMetadataTransformMetricName
    }

    def "Invalid standard metric and invalid protocol metric are detected"() {
        setup:
        // metrics to validate
        Set<LogicalMetric> logicalMetricsToValidate = [
                invalidProtocolMetricPostTransform,
                invalidLogicalMetric,
        ]

        when:
        generator.validateMetrics(logicalMetricsToValidate, logicalTable)

        then:
        BadApiRequestException e = thrown()
        errorMessageContainsNames(e.getMessage(), [invalidProtocolMetricBaseName, invalidLogicalMetricName] as Set)
    }

    def "Standard metrics, untransformed protocol metrics, and transformed protocol metrics which are all supported by the logical table are correctly validated"() {
        setup:
        Set<LogicalMetric> logicalMetricsToValidate = [
                validProtocolMetricPreTransform,
                validProtocolMetricPostTransform,
                validLogicalMetric,
        ]

        when:
        generator.validateMetrics(logicalMetricsToValidate, logicalTable)

        then:
        noExceptionThrown()
    }

    def "Mixing valid and invalid metrics still causes all invalid metrics to be caught"() {
        setup:
        Set<LogicalMetric> logicalMetricsToValidate = [
                validProtocolMetricPreTransform,
                validProtocolMetricPostTransform,
                invalidLogicalMetric,
                validLogicalMetric,
                invalidProtocolMetricPreTransform,
                invalidProtocolMetricPostTransform,
        ]

        when:
        generator.validateMetrics(logicalMetricsToValidate, logicalTable)

        then:
        BadApiRequestException e = thrown()
        errorMessageContainsNames(e.getMessage(), [invalidLogicalMetricName, invalidProtocolMetricBaseName] as Set)
    }

    def "ApiMetric appends granularity and creates LogicalMetric "() {
        setup:
        MetricTransformer metricTransformer = Mock(MetricTransformer);
        Protocol grainExpecting = new Protocol("ge", metricTransformer)
        ProtocolMetric protocolMetric = new ProtocolMetricImpl(
                new LogicalMetricInfo(baseMetadataTransformMetricName),
                new TemplateDruidQuery([], []),
                TEST_RSM,
                new ProtocolSupport([grainExpecting]),
        )
        MetricDictionary metricDictionary1 = new MetricDictionary()
        metricDictionary1.add(protocolMetric)
        String grainExpectingProtocolName = "ge"
        protocolDictionary.put(grainExpectingProtocolName, grainExpecting)
        ProtocolDictionary protocolDictionary1 = new ProtocolDictionary()
        protocolDictionary1.add(grainExpecting)
        ApiMetric apiMetric = new ApiMetric("baseMetric", "baseMetric", ["ge": "true"])
        TestApiMetricParser testParser = new TestApiMetricParser()
        testParser.setResultMetrics([apiMetric])
        ProtocolLogicalMetricGenerator generator = new ProtocolLogicalMetricGenerator(
                ApiMetricAnnotater.NO_OP_ANNOTATER,
                [grainExpectingProtocolName],
                testParser,
                protocolDictionary1,
        )
        1 * metricTransformer.apply(
                _,
                _,
                _,
                _
        ) >> {
            args ->
                assert ((Map<String, String>) args[3]).get(ProtocolLogicalMetricGenerator.GRANULARITY) == text
                return Mock(LogicalMetricImpl)
        }
        grainExpecting.getMetricTransformer() >> metricTransformer
        expect:
        generator.generateLogicalMetrics(text, expectedGrain, metricDictionary1) != null
        cleanup:
        protocolDictionary.remove(grainExpecting)
        where:
        expectedGrain           | text
        DefaultTimeGrain.DAY    | "day"
        DefaultTimeGrain.WEEK   | "week"
        DefaultTimeGrain.MONTH  | "month"
        AllGranularity.INSTANCE | "all"
    }


    def "ProtocolLogical Metric Generator appends granularity"() {
        setup:
        List<ApiMetric> metrics = generator.parseApiMetricQuery("foo()", expectedGrain)
        expect:
        metrics.
                every() {
                    it.contains(ProtocolLogicalMetricGenerator.GRANULARITY) && it.get
                    (ProtocolLogicalMetricGenerator.GRANULARITY) == text
                }
        where:
        expectedGrain           | text
        DefaultTimeGrain.DAY    | "day"
        DefaultTimeGrain.WEEK   | "week"
        DefaultTimeGrain.MONTH  | "month"
        AllGranularity.INSTANCE | "all"
    }
}
