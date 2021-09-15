// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints
import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Timeout

@Timeout(30)    // Fail test if hangs
class SlicesDebugServletSpec extends Specification {
    JerseyTestBinder jerseyTestBinder
    Interval interval = new Interval("2010-01-01/2500-12-31")

    def setup() {
        // Create the test web container to test the resources
        jerseyTestBinder = new JerseyTestBinder(SlicesServlet.class)

        AvailabilityTestingUtils.populatePhysicalTableCacheIntervals(jerseyTestBinder, interval)
    }

    def cleanup() {
        // Release the test web container
        jerseyTestBinder.tearDown()
    }

    def "test raw slice data endpoint"() {
        setup:
        String expectedResponse = """{
            "name":"all_pets",
            "timeGrain":"day",
            "timeZone":"UTC",
            "dimensions":
            [
                {"name":"breed","factName":"breed", "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/dimensions/breed", "intervals":["$interval"]},
                {"name":"species","factName":"class", "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/dimensions/species", "intervals":["$interval"]},
                {"name":"sex","factName":"sex", "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/dimensions/sex", "intervals":["$interval"]}
            ],
            "segmentInfo": {},
            "metrics":
            [
                {"name":"limbs", "intervals":["$interval"]}
            ]
        }"""

        when: "We send a request"
        String result = jerseyTestBinder.makeRequest("/slices/all_pets").get(String.class)

        then: "what we expect"
        GroovyTestUtils.compareJson(result, expectedResponse, JsonSortStrategy.SORT_BOTH)
    }
}
