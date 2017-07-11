// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.web.endpoints

import static com.yahoo.wiki.webservice.data.config.names.WikiDruidTableName.WIKITICKER

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils
import com.yahoo.bard.webservice.util.JsonSlurper
import com.yahoo.bard.webservice.util.JsonSortStrategy
import com.yahoo.bard.webservice.web.endpoints.SlicesServlet
import com.yahoo.wiki.webservice.application.WikiJerseyTestBinder

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

@Timeout(30)    // Fail test if hangs
class SlicesServletSpec extends Specification {
    JerseyTestBinder jtb
    JsonSlurper jsonSlurper = new JsonSlurper(JsonSortStrategy.SORT_BOTH)
    Interval interval = new Interval("2010-01-01/2500-12-31")

    def setup() {
        // Create the test web container to test the resources
        jtb = new WikiJerseyTestBinder(SlicesServlet.class)

        AvailabilityTestingUtils.populatePhysicalTableCacheIntervals(jtb, interval)
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
    }

    def "The slices are correctly configured, and the slices endpoint returns the appropriate metadata"() {
        setup:
        String sliceNameHour = WIKITICKER.asName()
        String expectedResponse = """{
            "rows":
            [
                {"timeGrain":"hour", "name":"$sliceNameHour", "uri":"http://localhost:9998/slices/$sliceNameHour"},
            ]
        }"""

        Map expectedJsonResult = jsonSlurper.parseText(expectedResponse) as Map
        Set expectedTableRows = expectedJsonResult.get("slices") as Set

        when: "We send a request"
        String result = makeRequest("/slices")
        Map jsonResult = (jsonSlurper.parseText(result) as Map)
        Set tableRows = jsonResult.get("slices") as Set

        then: "what we expect"
        expectedTableRows == tableRows
    }

    // Dimensions supported by the druidTopLevel table
    @Unroll
    def "The slice endpoint returns the correct data on #sliceName at granularity #granularity"() {
        setup:
        String expectedResponse = """{
            "name":"$sliceName",
            "timeGrain":"$granularity",
            "dimensions":
            [
                ${
                        dimensionNames.collect {"""
                                {
                                    "name":"$it",
                                    "uri":"http://localhost:9998/dimensions/$it",
                                    "intervals":["$interval"]
                                }
                        """}
                        .join(',')
                }
            ],
            "metrics":
            [
                ${
                        metricNames.collect {"""
                                {
                                    "name":"$it",
                                    "intervals":["$interval"]
                                }
                        """}
                        .join(',')
                }
            ]
        }"""

        Map expectedJsonResult = jsonSlurper.parseText(expectedResponse) as Map
        String expectedTableName = expectedJsonResult.get("name").toString()
        Set expectedDimensions = expectedJsonResult.get("dimensions") as Set
        Set expectedMetrics = expectedJsonResult.get("metrics") as Set

        when: "We send a request"
        String result = makeRequest("/slices/$sliceName")
        Map jsonResult = jsonSlurper.parseText(result) as Map
        String tableName = jsonResult.get("name").toString()
        Set dimensions = jsonResult.get("dimensions") as Set
        Set metrics = jsonResult.get("metrics") as Set

        then: "what we expect"
        tableName == expectedTableName
        dimensions == expectedDimensions
        metrics == expectedMetrics

        where:
        sliceName = WIKITICKER.asName().toLowerCase()
        granularity = "hour"
        dimensionNames = ("comment, countryIsoCode, regionIsoCode, page, user, isUnpatrolled, isNew, isRobot, isAnonymous," +
                " isMinor, namespace, channel, countryName, regionName, metroCode, cityName").split(',').collect { it.trim()}
        metricNames = "count, added, delta, deleted".split(',').collect {it.trim()}
    }

    String makeRequest(String target) {
        // Set target of call
        def httpCall = jtb.getHarness().target(target)

        // Make the call
        httpCall.request().get(String.class)
    }
}
