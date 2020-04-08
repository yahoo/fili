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
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.table.TestPhysicalTable
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetric
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetricAnnotater

import spock.lang.Specification
import spock.lang.Unroll

import java.sql.ResultSetMetaData

class ProtocolLogicalMetricGeneratorSpec extends Specification {

    static ResultSetMapper TEST_RSM = new NoOpResultSetMapper()

    String testProtocolName
    Protocol testProtocol
    ProtocolDictionary protocolDictionary

    TemplateDruidQuery emptyTdq

    String baseMetadataTransformMetricName
    String rawMetadataTransformMetricName
    ProtocolMetric baseMetric

    String validLogicalMetricName
    LogicalMetric validLogicalMetric

    String invalidLogicalMetricName
    LogicalMetric invalidLogicalMetric

    String validProtocolMetricBaseName
    String validProtocolMetricRawName
    ProtocolMetric validProtocolMetricPreTransform
    ProtocolMetric validProtocolMetricPostTransform

    String invalidProtocolMetricBaseName
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
        emptyTdq = new TemplateDruidQuery([], [])
        baseMetric = new ProtocolMetricImpl(
                new LogicalMetricInfo(baseMetadataTransformMetricName),
                emptyTdq,
                TEST_RSM,
                new ProtocolSupport([testProtocol]),
        )
        metricDictionary = new MetricDictionary()
        metricDictionary.add(baseMetric)

        // Prepare test metrics
        validLogicalMetricName = "validLogicalMetric"
        validLogicalMetric = new LogicalMetricImpl(
                new LogicalMetricInfo(validLogicalMetricName),
                emptyTdq,
                TEST_RSM
        )
        metricDictionary.add(validLogicalMetric)

        invalidLogicalMetricName = "invalidLogicalMetric"
        invalidLogicalMetric = new LogicalMetricImpl(
                new LogicalMetricInfo(invalidLogicalMetricName),
                emptyTdq,
                TEST_RSM
        )
        metricDictionary.add(invalidLogicalMetric)

        validProtocolMetricBaseName = "validProtocolMetric"
        validProtocolMetricRawName = "RAW_validProtocolMetric"

        validProtocolMetricPreTransform = new ProtocolMetricImpl(
                new LogicalMetricInfo(validProtocolMetricBaseName),
                emptyTdq,
                TEST_RSM
        )
        metricDictionary.add(validProtocolMetricPreTransform)

        validProtocolMetricPostTransform = new ProtocolMetricImpl(
                new GeneratedMetricInfo(validProtocolMetricRawName, validProtocolMetricBaseName),
                emptyTdq,
                TEST_RSM
        )

        invalidProtocolMetricBaseName = "invalidProtocolMetric"
        invalidProtocolMetricRawName = "RAW_invalidProtocolMetric"

        invalidProtocolMetricPreTransform = new ProtocolMetricImpl(
                new LogicalMetricInfo(invalidProtocolMetricBaseName),
                emptyTdq,
                TEST_RSM
        )
        metricDictionary.add(invalidProtocolMetricPreTransform)

        invalidProtocolMetricPostTransform = new ProtocolMetricImpl(
                new GeneratedMetricInfo(invalidProtocolMetricRawName, invalidProtocolMetricBaseName),
                emptyTdq,
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

    def "ProtocolLogical Metric Generator appends granularity"() {
        setup:
        List<ApiMetric> metrics = generator.parseApiMetricQueryWithGranularity("foo()", expectedGrain)
        expect:
        metrics.every() {it.contains(ProtocolLogicalMetricGenerator.GRANULARITY) && it.get(ProtocolLogicalMetricGenerator.GRANULARITY) == text}
        where:
        expectedGrain           | text
        DefaultTimeGrain.DAY    | "day"
        DefaultTimeGrain.WEEK   | "week"
        DefaultTimeGrain.MONTH  | "month"
        AllGranularity.INSTANCE | "all"
    }
}
