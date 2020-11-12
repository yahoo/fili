// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.having.antlr

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.metric.protocol.GeneratedMetricInfo
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.having.Having
import com.yahoo.bard.webservice.druid.model.having.HavingType
import com.yahoo.bard.webservice.web.ApiHaving
import com.yahoo.bard.webservice.web.HavingOperation
import com.yahoo.bard.webservice.web.apirequest.generator.metric.antlr.ProtocolAntlrApiMetricParser
import com.yahoo.bard.webservice.web.apirequest.generator.metric.antlr.ProtocolAntlrApiMetricParserSpec
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetric

import spock.lang.Specification
import spock.lang.Unroll

class AntlrHavingGeneratorSpec extends Specification {

    ProtocolAntlrApiMetricParser metricParser = new ProtocolAntlrApiMetricParser()
    MetricDictionary metricDictionary = new MetricDictionary()
    AntlrHavingGenerator generator = new AntlrHavingGenerator(metricDictionary)
    Set<LogicalMetric> metrics = [] as Set


    def setup() {
        def apiMetrics = []
        String key
        key = "one(bar=baz)"
        ApiMetric expectedMetric = new ApiMetric(key.replace(" ", ""), "one", ["bar": "baz"])
        apiMetrics.add(expectedMetric)
        key = "two(  )"
        expectedMetric = new ApiMetric(key.replace(" ", ""), "two", [:])
        apiMetrics.add(expectedMetric)
        key = " three "
        expectedMetric = new ApiMetric(key.replace(" ", ""), "three", [:])
        apiMetrics.add(expectedMetric)
        key = "four(bar=baz, one = two)"
        expectedMetric = new ApiMetric(key.replace(" ", ""), "four", ["bar": "baz", "one": "two"])
        apiMetrics.add(expectedMetric)

        apiMetrics.each {
            LongSumAggregation aggregation = new LongSumAggregation(it.getBaseApiMetricId(), "foo")
            TemplateDruidQuery templateDruidQuery = new TemplateDruidQuery([aggregation], [])
            GeneratedMetricInfo generatedMetricInfo = new GeneratedMetricInfo(it.getRawName(), it.getBaseApiMetricId())
            LogicalMetric logicalMetric = new LogicalMetricImpl(generatedMetricInfo, templateDruidQuery, new NoOpResultSetMapper())
            metricDictionary.add(logicalMetric)
            metrics.add(logicalMetric)
        }

        1==1
    }

    @Unroll
    def "Generator produces a having with metric #metric, operation #op, and #params from text: #text"() {
        setup:
        Map<LogicalMetric, Set<ApiHaving>> havingMap = generator.apply(text, metrics)
        LogicalMetric logicalMetric = havingMap.keySet().stream().findFirst().get();
        Set<ApiHaving> havings = havingMap.get(logicalMetric)
        ApiHaving having = havings.stream().findFirst().get()
        String metricPart = text.split("-")[0]


        expect:
        havingMap.size() == 1
        havings.size() == 1
        having. values == values
        having.metric.getName() == (metricPart.replace(" ", ""))
        having.getOperation() == HavingOperation.fromString(op)

        where:
        text                                   | metric  | params         | op        | values
        // unquoted values

        "two ( )-gt[4]"                        | "two"   | [:]            | "gt"      | [4]
        "three-greaterThan[1E-100]"            | "two"   | [:]            | "gt"      | [1E-100]
        "four(bar=baz, one=two)-eq[5, 1E-100]" | "two"   | [:]            | "equals"  | [5, 1E-100]
        "two ( )-notEquals[1E-100]"            | "two"   | [:]            | "neq"     | [1E-100]
        // Note, having support for between appears not to be fully implemented
        "one(bar=baz)-between[1 , 2]"          | "one"   | ["bar": "baz"] | "between" | [1, 2]
        "three-bet[-3E-33, 21222]"             | "three" | [:]            | "between" | [-3.0E-33, 21222]
        "three-notBetween[1,  2]  "            | "three" | [:]            | "nbet"    | [1, 2]
    }}
