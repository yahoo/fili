// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy
import com.yahoo.bard.webservice.web.endpoints.TablesServlet
import com.yahoo.wiki.webservice.application.WikiJerseyTestBinder
import com.yahoo.wiki.webservice.data.config.names.WikiDruidTableName

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

@Timeout(30)
// Fail test if hangs
class TablesServletSpec extends Specification {
    JerseyTestBinder jerseyTestBinder

    def setup() {
        // Create the test web container to test the resources
        jerseyTestBinder = new WikiJerseyTestBinder(TablesServlet.class)
        AvailabilityTestingUtils.populatePhysicalTableCacheIntervals(
                jerseyTestBinder,
                new Interval("2018-01-01/2018-02-01"),
                [WikiDruidTableName.WIKITICKER.asName()] as Set
        )
    }

    def cleanup() {
        // Release the test web container
        jerseyTestBinder.tearDown()
    }

    def "print the details of all the tables in the Druid instance"() {
        setup:
        String tableName = WikiDruidTableName.WIKITICKER.asName()
        String expectedResponse = """{
                                        "rows": [
                                            {
                                                    "category":"General",
                                                    "granularity":"hour",
                                                    "name":"$tableName",
                                                    "longName":"$tableName",
                                                    "uri":"http://localhost:9998/tables/$tableName/hour"
                                            },
                                            {
                                                    "category":"General",
                                                    "granularity":"all",
                                                    "name":"$tableName",
                                                    "longName":"$tableName",
                                                    "uri":"http://localhost:9998/tables/$tableName/all"
                                            }
                                        ]
                                    }"""

        expect: "The result of the query is as expected"
        GroovyTestUtils.compareJson(makeRequest("/tables"), expectedResponse, JsonSortStrategy.SORT_BOTH)
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
                                                "uri":"http://localhost:9998/tables/$tableName/hour"
                                            },
                                            {
                                                "category":"General",
                                                "granularity":"all",
                                                "name":"$tableName",
                                                "longName":"$tableName",
                                                "uri":"http://localhost:9998/tables/$tableName/all"
                                            }
                                        ]
                                    }"""

        expect: "The request returns the correct JSON result"
        GroovyTestUtils.compareJson(makeRequest("/tables/$tableName"), expectedResponse, JsonSortStrategy.SORT_BOTH)

        where:
        tableName = WikiDruidTableName.WIKITICKER.asName()
    }

    //This test is a sample of how we test various table endpoints at differing granularities
    @Unroll
    def "Querying for table #tableName at granularity #granularity returns that table's information"() {
        setup:
        List<String> dimensionNames = ("channel, cityName, comment, countryIsoCode, countryName, isAnonymous, isMinor, isNew, isRobot, isUnpatrolled, metroCode, namespace, page, regionIsoCode, regionName, user").split(',').collect { it.trim() }

        List<String> metricNames = "count, added, delta, deleted, user_unique".split(',').collect { it.trim() }
        String expectedResponse = """{
                                        "availableIntervals":["2018-01-01T00:00:00.000Z/2018-02-01T00:00:00.000Z"],
                                        "name":"$tableName",
                                        "longName":"$tableName",
                                        "granularity":"hour",
                                        "category": "General",
                                        "retention": "P1Y",
                                        "description": "$tableName",
                                        "dimensions": [
                                            ${
                                                dimensionNames.collect {
                                                    """{
                                                        "category": "General",
                                                        "name": "$it",
                                                        "longName": "$it",
                                                        "cardinality": 0,
                                                        "uri": "http://localhost:9998/dimensions/$it"
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
                                                        "uri": "http://localhost:9998/metrics/$it"
                                                    }""" 
                                                }
                                                .join(',')
                                            }
                                        ]
                                    }"""

        when: "We send a request"
        String result = makeRequest("/tables/$tableName/$granularity")

        then: "what we expect"
        GroovyTestUtils.compareJson(result, expectedResponse, JsonSortStrategy.SORT_BOTH)

        where:
        tableName = WikiDruidTableName.WIKITICKER.asName()
        granularity = "hour"
    }

    String makeRequest(String target) {
        jerseyTestBinder
                .getHarness()
                .target(target)
                .request()
                .get(String.class)
    }
}
