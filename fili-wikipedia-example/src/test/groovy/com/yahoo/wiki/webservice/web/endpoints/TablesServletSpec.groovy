// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.web.endpoints

import static com.yahoo.wiki.webservice.data.config.names.WikiLogicalTableName.WIKIPEDIA

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
        String tableName = WIKIPEDIA.asName()
        String expectedResponse = """{
                                        "rows": [
                                            {
                                                    "category":"General",
                                                    "granularity":"hour",
                                                    "name":"$tableName",
                                                    "longName":"$tableName",
                                                    "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableName/hour"
                                            },
                                            {
                                                    "category":"General",
                                                    "granularity":"day",
                                                    "name":"$tableName",
                                                    "longName":"$tableName",
                                                    "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableName/day"
                                            },
                                            {
                                                    "category":"General",
                                                    "granularity":"all",
                                                    "name":"$tableName",
                                                    "longName":"$tableName",
                                                    "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableName/all"
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
        String expectedResponse = """{
                                         "rows": [
                                             {
                                                 "category":"General",
                                                 "granularity":"hour",
                                                 "name":"$tableName",
                                                 "longName":"$tableName",
                                                 "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableName/hour"
                                             },
                                             {
                                                 "category":"General",
                                                 "granularity":"day",
                                                 "name":"$tableName",
                                                 "longName":"$tableName",
                                                 "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableName/day"
                                             },
                                             {
                                                 "category":"General",
                                                 "granularity":"all",
                                                 "name":"$tableName",
                                                 "longName":"$tableName",
                                                 "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/tables/$tableName/all"
                                             }
                                         ]
                                     }"""

        expect: "The request returns the correct JSON result"
        GroovyTestUtils.compareJson(
                jerseyTestBinder.makeRequest("/tables/$tableName").get(String.class),
                expectedResponse,
                JsonSortStrategy.SORT_BOTH
        )

        where:
        tableName = WIKIPEDIA.asName()
    }

    //This test is a sample of how we test various table endpoints at differing granularities
    @Unroll
    def "Querying for table #tableName at granularity #granularity returns that table's information"() {
        setup:
        List<String> dimensionNames = ("comment, countryIsoCode, regionIsoCode, page, user, isUnpatrolled, isNew, isRobot, isAnonymous," +
                " isMinor, namespace, channel, countryName, regionName, metroCode, cityName").split(',').collect { it.trim()}

        List<String> metricNames = "count, added, delta, deleted".split(',').collect{ it.trim()}
        String expectedResponse = """{
                                        "availableIntervals":[],
                                        "name":"$tableName",
                                        "longName":"$tableName",
                                        "granularity":"hour",
                                        "category": "General",
                                        "retention": "",
                                        "description": "$tableName",
                                        "dimensions": [
                                            ${
                                                dimensionNames.collect {
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
                                                metricNames.collect {
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
        String result = jerseyTestBinder.makeRequest("/tables/$tableName/$granularity").get(String.class)

        then: "what we expect"
        GroovyTestUtils.compareJson(result, expectedResponse, JsonSortStrategy.SORT_BOTH)

        where:
        tableName = WIKIPEDIA.asName().toLowerCase()
        granularity = "hour"
    }
}
