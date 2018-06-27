// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy
import com.yahoo.bard.webservice.web.endpoints.TablesServlet
import com.yahoo.wiki.webservice.application.WikiJerseyTestBinder

import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

@Timeout(30)    // Fail test if hangs
class TablesServletSpec extends Specification {
    JerseyTestBinder jerseyTestBinder

    def setup() {
        // Create the test web container to test the resources
        jerseyTestBinder = new WikiJerseyTestBinder(TablesServlet.class)
    }

    def cleanup() {
        // Release the test web container
        jerseyTestBinder.tearDown()
    }

    def "print the details of all the tables in the Druid instance"() {
        setup:
        String tableNameOne = "wikipedia"
        String tableNameTwo = "logicaltabletester"
        String expectedResponse = """{
                                        "rows": [
                                            {
                                                    "category":"General",
                                                    "granularity":"hour",
                                                    "name":"$tableNameOne",
                                                    "longName":"$tableNameOne",
                                                    "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameOne/hour"
                                            },
                                            {
                                                    "category":"General",
                                                    "granularity":"day",
                                                    "name":"$tableNameOne",
                                                    "longName":"$tableNameOne",
                                                    "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameOne/day"
                                            },
                                            {
                                                    "category":"General",
                                                    "granularity":"all",
                                                    "name":"$tableNameOne",
                                                    "longName":"$tableNameOne",
                                                    "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameOne/all"
                                            },
                                                                                        {
                                                    "category":"General",
                                                    "granularity":"hour",
                                                    "name":"$tableNameTwo",
                                                    "longName":"$tableNameTwo",
                                                    "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameTwo/hour"
                                            },
                                            {
                                                    "category":"General",
                                                    "granularity":"day",
                                                    "name":"$tableNameTwo",
                                                    "longName":"$tableNameTwo",
                                                    "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameTwo/day"
                                            }
                                        ]
                                    }"""

        expect: "The result of the query is as expected"
        GroovyTestUtils.compareJson(
                jerseyTestBinder.makeRequest("/tables").get(String.class),
                expectedResponse,
                JsonSortStrategy.SORT_BOTH
        )
    }

    @Unroll
    def "Requesting table #tableName gives us the correct details about #tableName"() {
        setup:
        String expectedResponseOne = """{
                                         "rows": [
                                             {
                                                 "category":"General",
                                                 "granularity":"hour",
                                                 "name":"$tableNameOne",
                                                 "longName":"$tableNameOne",
                                                 "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameOne/hour"
                                             },
                                             {
                                                 "category":"General",
                                                 "granularity":"day",
                                                 "name":"$tableNameOne",
                                                 "longName":"$tableNameOne",
                                                 "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameOne/day"
                                             },
                                             {
                                                 "category":"General",
                                                 "granularity":"all",
                                                 "name":"$tableNameOne",
                                                 "longName":"$tableNameOne",
                                                 "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameOne/all"
                                             }
                                         ]
                                     }"""
        String expectedResponseTwo = """{
                                         "rows": [
                                             {
                                                 "category":"General",
                                                 "granularity":"hour",
                                                 "name":"$tableNameTwo",
                                                 "longName":"$tableNameTwo",
                                                 "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameTwo/hour"
                                             },
                                             {
                                                 "category":"General",
                                                 "granularity":"day",
                                                 "name":"$tableNameTwo",
                                                 "longName":"$tableNameTwo",
                                                 "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameTwo/day"
                                             }
                                         ]
                                     }"""

        expect: "The request returns the correct JSON result"
        GroovyTestUtils.compareJson(
                jerseyTestBinder.makeRequest("/tables/$tableNameOne").get(String.class),
                expectedResponseOne,
                JsonSortStrategy.SORT_BOTH
        )

        GroovyTestUtils.compareJson(
                jerseyTestBinder.makeRequest("/tables/$tableNameTwo").get(String.class),
                expectedResponseTwo,
                JsonSortStrategy.SORT_BOTH
        )

        where:
        tableNameOne = "wikipedia"
        tableNameTwo = "logicaltabletester"
    }

    //This test is a sample of how we test various table endpoints at differing granularities
    @Unroll
    def "Querying for table #tableName at granularity #granularity returns that table's information"() {
        setup:
        List<String> dimensionNamesOne = ("comment, countryIsoCode, regionIsoCode, page, user, isUnpatrolled, isNew, isRobot, isAnonymous," +
                " isMinor, namespace, channel, countryName, regionName, metroCode, cityName").split(',').collect { it.trim()}
        List<String> metricNamesOne = "count, added, delta, deleted, metrictestertwo".split(',').collect{ it.trim()}
        List<String> dimensionNamesTwo = ("comment, countryIsoCode").split(',').collect { it.trim()}
        List<String> metricNamesTwo = "count, added, delta, deleted, bigthetasketch, metrictesterone".split(',').collect{ it.trim()}

        String expectedResponseOne = """{
                                        "availableIntervals":[],
                                        "name":"$tableNameOne",
                                        "longName":"$tableNameOne",
                                        "granularity":"day",
                                        "category": "General",
                                        "retention": "P1Y",
                                        "description": "$tableNameOne",
                                        "dimensions": [
                                            ${dimensionNamesOne.collect { 
                                                    """{
                                                    "category": "General",
                                                    "name": "$it",
                                                    "longName": "wiki $it",
                                                    "cardinality": 0,
                                                    "storageStrategy":"loaded",
                                                    "uri": "http://localhost:${jerseyTestBinder.getHarness().getPort()}/dimensions/$it"
                                                    }""" 
                                                }
                                                .join(',')
                                            }
                                        ],
                                        "metrics": [
                                            ${
                                                metricNamesOne.collect {
                                                    """{
                                                        "category": "General",
                                                        "name": "$it",
                                                        "longName": "$it",
                                                        "type": "number",
                                                        "uri": "http://localhost:${jerseyTestBinder.getHarness().getPort()}/metrics/$it"
                                                    }""" 
                                                }
                                                .join(',')
                                            }
                                        ]
                                    }"""

        String expectedResponseTwo = """{
                                        "availableIntervals":[],
                                        "name":"$tableNameTwo",
                                        "longName":"$tableNameTwo",
                                        "granularity":"day",
                                        "category": "General",
                                        "retention": "P1Y",
                                        "description": "$tableNameTwo",
                                        "dimensions": [
                                            ${
                                                dimensionNamesTwo.collect {
                                                    """{
                                                    "category": "General",
                                                    "name": "$it",
                                                    "longName": "wiki $it",
                                                    "cardinality": 0,
                                                    "storageStrategy":"loaded",
                                                    "uri": "http://localhost:${jerseyTestBinder.getHarness().getPort()}/dimensions/$it"
                                                    }"""
                                                }
                                                .join(',')
                                            }
                                        ],
                                        "metrics": [
                                            ${
                                                metricNamesTwo.collect {
                                                    """{
                                                        "category": "General",
                                                        "name": "$it",
                                                        "longName": "$it",
                                                        "type": "number",
                                                        "uri": "http://localhost:${jerseyTestBinder.getHarness().getPort()}/metrics/$it"
                                                    }"""
                                                }
                                                .join(',')
                                            }
                                        ]
                                    }"""


        when: "We send a request"
        String resultOne = jerseyTestBinder.makeRequest("/tables/$tableNameOne/$granularity").get(String.class)
        String resultTwo = jerseyTestBinder.makeRequest("/tables/$tableNameTwo/$granularity").get(String.class)

        then: "what we expect"
        GroovyTestUtils.compareJson(resultOne, expectedResponseOne, JsonSortStrategy.SORT_BOTH)
        GroovyTestUtils.compareJson(resultTwo, expectedResponseTwo, JsonSortStrategy.SORT_BOTH)

        where:
        tableNameOne= "wikipedia"
        tableNameTwo= "logicaltabletester"
        granularity = "DAY"

    }
}
