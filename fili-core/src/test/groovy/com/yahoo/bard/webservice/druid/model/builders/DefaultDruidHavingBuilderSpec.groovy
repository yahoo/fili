// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.builders

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.druid.model.having.Having
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.web.ApiHaving

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

public class DefaultDruidHavingBuilderSpec extends Specification {

    @Shared QueryBuildingTestingResources resources

    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()
    DefaultDruidHavingBuilder druidHavingBuilder

    def setupSpec() {
        resources = new QueryBuildingTestingResources()
    }

    def setup() {
        druidHavingBuilder = DefaultDruidHavingBuilder.INSTANCE
    }

    def "No havings returns null"() {
        expect:
        druidHavingBuilder.buildHavings([:]) == null
    }

    @Unroll
    def "buildHaving turns the query #havingString into the JSON #expectedJson"() {

        setup:
        ApiHaving apiHaving = new ApiHaving(havingString, resources.metricDictionary)
        Having having = druidHavingBuilder.buildHaving(metric, apiHaving)

        expect:
        GroovyTestUtils.compareJson(MAPPER.writer().writeValueAsString(having), expectedJson)

        where:
        metric       | havingString         | expectedJson
        resources.m1 | "metric1-eq[1]"      | """{"type": "equalTo", "aggregation": "metric1", "value": 1}"""
        resources.m1 | "metric1-eq[1.0]"    | """{"type": "equalTo", "aggregation": "metric1", "value": 1.0}"""
        resources.m2 | "metric2-eq[1.0]"    | """{"type": "equalTo", "aggregation": "metric2", "value": 1.0}"""
        resources.m2 | "metric2-eq[1,2,3]"  | """{
                                                 "type": "or",
                                                 "havingSpecs": [
                                                   { "type": "equalTo", "aggregation": "metric2", "value": 1 },
                                                   { "type": "equalTo", "aggregation": "metric2", "value": 2 },
                                                   { "type": "equalTo", "aggregation": "metric2", "value": 3 }
                                                 ]
                                               }"""
        resources.m3 | "metric3-lte[1,2,3]" | """{
                                                  "type": "not",
                                                  "havingSpec": {
                                                    "type": "or",
                                                    "havingSpecs": [
                                                      { "type": "greaterThan", "aggregation": "metric3", "value": 1 },
                                                      { "type": "greaterThan", "aggregation": "metric3", "value": 2 },
                                                      { "type": "greaterThan", "aggregation": "metric3", "value": 3 }
                                                    ]
                                                  }
                                                }"""
        resources.m3 | "metric3-bet[0,5]"   | """{
                                                   "type": "and",
                                                   "havingSpecs": [
                                                     {
                                                       "type": "or",
                                                       "havingSpecs": [
                                                         {"aggregation": "metric3", "type": "greaterThan", "value": 0},
                                                         {"aggregation": "metric3", "type": "equalTo", "value": 0}
                                                       ]

                                                     },
                                                     {
                                                       "type": "or",
                                                       "havingSpecs": [
                                                         {"aggregation": "metric3", "type": "lessThan", "value": 5},
                                                         {"aggregation": "metric3", "type": "equalTo", "value": 5}
                                                       ]

                                                     }
                                                   ]

                                               }"""
        resources.m3 | "metric3-nbet[10,50]" | """{
                                                    "type": "not",
                                                    "havingSpec": {
                                                      "type": "and",
                                                      "havingSpecs": [

                                                        {
                                                          "type": "or",
                                                          "havingSpecs": [
                                                            {"aggregation": "metric3", "type": "greaterThan", "value": 10},
                                                            {"aggregation": "metric3", "type": "equalTo", "value": 10}
                                                          ]

                                                        },
                                                        {
                                                          "type": "or",
                                                          "havingSpecs": [
                                                            {"aggregation": "metric3", "type": "lessThan", "value": 50},
                                                            {"aggregation": "metric3", "type": "equalTo", "value": 50}
                                                          ]
                                                        }
                                                      ]

                                                    }
                                                }"""
    }

    def "Test build multi-metric havings"() {

        setup:
        Map<LogicalMetric, Set<String>> havingStrings = [
                (resources.m1): ["metric1-lt[10]", "metric1-gt[2]"] as Set,
                (resources.m2): ["metric2-eq[14.7,8]"] as Set,
                (resources.m3): ["metric3-neq[14.7,8,37]"] as Set,
        ]

        Map<LogicalMetric, Set<ApiHaving>> metricMap = [:]

        havingStrings.each {
            metricMap[it.key] = [] as Set<ApiHaving>
            it.value.each { havingString ->
                ApiHaving apiHaving = new ApiHaving(havingString, resources.metricDictionary)
                metricMap[it.key].add(apiHaving)
            }
        }

        Having having = druidHavingBuilder.buildHavings(metricMap)

        String expectedJson = """{
                                   "type": "and",
                                   "havingSpecs": [
                                     {
                                       "type": "and",
                                       "havingSpecs": [
                                         { "aggregation": "metric1", "type": "lessThan",    "value": 10 },
                                         { "aggregation": "metric1", "type": "greaterThan", "value": 2 }
                                       ]
                                     },
                                     {
                                       "type": "or",
                                       "havingSpecs": [
                                         { "aggregation": "metric2", "type": "equalTo", "value": 14.7 },
                                         { "aggregation": "metric2", "type": "equalTo", "value": 8 }
                                       ]
                                     },
                                     {
                                       "type": "not",
                                       "havingSpec": {
                                         "type": "or",
                                         "havingSpecs": [
                                           { "aggregation": "metric3", "type": "equalTo", "value": 14.7 },
                                           { "aggregation": "metric3", "type": "equalTo", "value": 8 },
                                           { "aggregation": "metric3", "type": "equalTo", "value": 37 }
                                         ]
                                       }
                                     }
                                   ]
                                 }"""

        expect:
        GroovyTestUtils.compareJson(MAPPER.writer().writeValueAsString(having), expectedJson)
    }
}
