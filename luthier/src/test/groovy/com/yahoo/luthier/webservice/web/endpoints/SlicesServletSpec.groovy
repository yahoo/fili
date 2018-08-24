// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils
import com.yahoo.bard.webservice.util.JsonSlurper
import com.yahoo.bard.webservice.util.JsonSortStrategy
import com.yahoo.bard.webservice.web.endpoints.SlicesServlet
import com.yahoo.luthier.webservice.application.LuthierJerseyTestBinder

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

@Timeout(30)    // Fail test if hangs
class SlicesServletSpec extends Specification {
    JerseyTestBinder jerseyTestBinder
    JsonSlurper jsonSlurper = new JsonSlurper(JsonSortStrategy.SORT_BOTH)
    Interval interval = new Interval("2010-01-01/2500-12-31")

    def setup() {
        // Create the test web container to test the resources
        jerseyTestBinder = new LuthierJerseyTestBinder(SlicesServlet.class)

        AvailabilityTestingUtils.populatePhysicalTableCacheIntervals(jerseyTestBinder, interval)
    }

    def cleanup() {
        // Release the test web container
        jerseyTestBinder.tearDown()
    }

    def "The slices are correctly configured, and the slices endpoint returns the appropriate metadata"() {
        setup:
        String sliceNameOne = "wikiticker"
        String sliceNameTwo = "physicalTableTester"
        String expectedResponse = """{
            "rows":
            [
                {"timeGrain":"hour", "name":"$sliceNameOne", "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/slices/$sliceNameOne"},
                {"timeGrain":"day", "name":"$sliceNameTwo", "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/slices/$sliceNameTwo"}
            ]
        }"""

        Map expectedJsonResult = jsonSlurper.parseText(expectedResponse) as Map
        Set expectedTableRows = expectedJsonResult.get("slices") as Set

        when: "We send a request"
        String result = jerseyTestBinder.makeRequest("/slices").get(String.class)
        Map jsonResult = (jsonSlurper.parseText(result) as Map)
        Set tableRows = jsonResult.get("slices") as Set

        then: "what we expect"
        expectedTableRows == tableRows
    }

    // Dimensions supported by the druidTopLevel table
    @Unroll
    def "The slice endpoint returns the correct data on #sliceName at granularity #granularity"() {
        setup:
        String expectedResponseOne = """{
            "name":"$sliceNameOne",
            "timeGrain":"$granularityOne",
            "dimensions":
            [
                ${
            dimensionNamesOne.collect {"""
                        {
                            "name":"$it",
                            "factName": "$it",
                            "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/dimensions/$it",
                            "intervals":["$interval"]
                        }
                    """}
            .join(',')
        }
            ],
            "metrics":
            [
                ${
            metricNamesOne.collect {"""
                        {
                            "name":"$it",
                            "intervals":["$interval"]
                        }
                    """}
            .join(',')
        }
            ]
        }"""

        String expectedResponseTwo = """{
            "name":"$sliceNameTwo",
            "timeGrain":"$granularityTwo",
            "dimensions":
            [
                ${
                    dimensionNamesTwo.collect {"""
                        {
                            "name":"$it",
                            "factName": "$it",
                            "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/dimensions/$it",
                            "intervals":["$interval"]
                        }
                    """}
                    .join(',')
                }
            ],
            "metrics":
            [
                ${
                    metricNamesTwo.collect {"""
                        {
                            "name":"$it",
                            "intervals":["$interval"]
                        }
                    """}
                    .join(',')
                }
            ]
        }"""

        Map expectedJsonResultOne  = jsonSlurper.parseText(expectedResponseOne) as Map
        String expectedTableNameOne  = expectedJsonResultOne.get("name").toString()
        Set expectedDimensionsOne  = expectedJsonResultOne.get("dimensions") as Set
        Set expectedMetricsOne  = expectedJsonResultOne.get("metrics") as Set

        Map expectedJsonResultTwo  = jsonSlurper.parseText(expectedResponseTwo) as Map
        String expectedTableNameTwo  = expectedJsonResultTwo.get("name").toString()
        Set expectedDimensionsTwo  = expectedJsonResultTwo.get("dimensions") as Set
        Set expectedMetricsTwo  = expectedJsonResultTwo.get("metrics") as Set

        when: "We send a request"

        String resultOne  = jerseyTestBinder.makeRequest("/slices/$sliceNameOne").get(String.class)
        Map jsonResultOne  = jsonSlurper.parseText(resultOne) as Map
        String tableNameOne  = jsonResultOne.get("name").toString()
        Set dimensionsOne  = jsonResultOne.get("dimensions") as Set
        Set metricsOne = jsonResultOne.get("metrics") as Set

        String resultTwo  = jerseyTestBinder.makeRequest("/slices/$sliceNameTwo").get(String.class)
        Map jsonResultTwo  = jsonSlurper.parseText(resultTwo) as Map
        String tableNameTwo  = jsonResultTwo .get("name").toString()
        Set dimensionsTwo  = jsonResultTwo .get("dimensions") as Set
        Set metricsTwo = jsonResultTwo .get("metrics") as Set

        then: "what we expect"
        tableNameOne == expectedTableNameOne
        dimensionsOne  == expectedDimensionsOne
        metricsOne  == expectedMetricsOne

        tableNameTwo == expectedTableNameTwo
        dimensionsTwo  == expectedDimensionsTwo
        metricsTwo  == expectedMetricsTwo

        where:
        sliceNameOne = "wikiticker"
        granularityOne = "hour"
        dimensionNamesOne = ("comment, countryIsoCode, regionIsoCode, page, user, isUnpatrolled, isNew, isRobot, isAnonymous," +
                " isMinor, namespace, channel, countryName, regionName, metroCode, cityName").split(',').collect { it.trim()}
        metricNamesOne = "added, delta, deleted".split(',').collect {it.trim()}

        sliceNameTwo = "physicalTableTester"
        granularityTwo = "hour"
        dimensionNamesTwo = ("comment, countryIsoCode").split(',').collect { it.trim()}
        metricNamesTwo = "count, added".split(',').collect {it.trim()}
    }
}
