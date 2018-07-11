// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy
import com.yahoo.bard.webservice.web.endpoints.TablesServlet
import com.yahoo.luthier.webservice.application.LuthierJerseyTestBinder

import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

@Timeout(30)    // Fail test if hangs
class TablesServletSpec extends Specification {
    JerseyTestBinder jerseyTestBinder

    def setup() {
        // Create the test web container to test the resources
        jerseyTestBinder = new LuthierJerseyTestBinder(TablesServlet.class)
    }

    def cleanup() {
        // Release the test web container
        jerseyTestBinder.tearDown()
    }

    def "print the details of all the tables in the Druid instance"() {
        setup:
        String tableNameOne = "wikipedia"
        String tableNameTwo = "logicaltabletesterone"
        String tableNameThree = "logicaltabletestertwo"
        String tableNameFour = "logicaltabletesterthree"
        String expectedResponse = """{
                                        "rows": [
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
                                                    "name":"$tableNameTwo",
                                                    "longName":"$tableNameTwo",
                                                    "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameTwo/all"
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
                                            },
                                            {
                                                    "category":"General",
                                                    "granularity":"hour",
                                                    "name":"$tableNameThree",
                                                    "longName":"$tableNameThree",
                                                    "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameThree/hour"
                                            },
                                            {
                                                    "category":"General",
                                                    "granularity":"hour",
                                                    "name":"$tableNameFour",
                                                    "longName":"$tableNameFour",
                                                    "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameFour/hour"
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
                                             },
                                             {
                                                 "category":"General",
                                                 "granularity":"all",
                                                 "name":"$tableNameTwo",
                                                 "longName":"$tableNameTwo",
                                                 "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameTwo/all"
                                             }
                                         ]
                                     }"""

        String expectedResponseThree = """{
                                         "rows": [
                                             {
                                                 "category":"General",
                                                 "granularity":"hour",
                                                 "name":"$tableNameThree",
                                                 "longName":"$tableNameThree",
                                                 "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameThree/hour"
                                             }
                                         ]
                                     }"""

        String expectedResponseFour = """{
                                         "rows": [
                                             {
                                                 "category":"General",
                                                 "granularity":"hour",
                                                 "name":"$tableNameFour",
                                                 "longName":"$tableNameFour",
                                                 "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableNameFour/hour"
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

        GroovyTestUtils.compareJson(
                jerseyTestBinder.makeRequest("/tables/$tableNameThree").get(String.class),
                expectedResponseThree,
                JsonSortStrategy.SORT_BOTH
        )

        GroovyTestUtils.compareJson(
                jerseyTestBinder.makeRequest("/tables/$tableNameFour").get(String.class),
                expectedResponseFour,
                JsonSortStrategy.SORT_BOTH
        )

        where:
        tableNameOne = "wikipedia"
        tableNameTwo = "logicaltabletesterone"
        tableNameThree = "logicaltabletestertwo"
        tableNameFour = "logicaltabletesterthree"
    }

    //This test is a sample of how we test various table endpoints at differing granularities
    @Unroll
    def "Querying for table #tableName at granularity #granularity returns that table's information"() {
        setup:
        List<String> dimensionNamesOne = ("comment, countryIsoCode, regionIsoCode, page, user, isUnpatrolled, isNew, isRobot, isAnonymous," +
                " isMinor, namespace, channel, countryName, regionName, metroCode, cityName").split(',').collect { it.trim()}
        List<String> metricNamesOne = ("count, added, delta, deleted").split(',').collect{ it.trim()}
        List<String> dimensionNamesTwo = ("comment, countryIsoCode, regionIsoCode, page, user, isUnpatrolled, isNew, isRobot, isAnonymous," +
                " isMinor, namespace, channel, countryName, regionName, metroCode, cityName").split(',').collect { it.trim()}
        List<String> metricNamesTwo = ("count, added, delta, deleted, averageaddedperhour, averagedeletedperhour," +
                "plusavgaddeddeleted, minusaddeddelta, cardonpage, bigthetasketch").split(',').collect{ it.trim()}

        String expectedResponseOne = """{
                                        "availableIntervals":[],
                                        "name":"$tableNameOne",
                                        "longName":"$tableNameOne",
                                        "granularity":"hour",
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
                                        "granularity":"hour",
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
        tableNameTwo= "logicaltabletesterone"
        granularity = "HOUR"

    }
}
