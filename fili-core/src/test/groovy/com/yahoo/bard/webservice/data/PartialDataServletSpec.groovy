// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE
import static com.yahoo.bard.webservice.config.BardFeatureFlag.PARTIAL_DATA

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.cache.TestDataCache
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.web.endpoints.DataServlet

import org.joda.time.Interval

import spock.lang.IgnoreIf
import spock.lang.Specification

import javax.ws.rs.BadRequestException

class PartialDataServletSpec extends Specification {

    JerseyTestBinder jtb
    static boolean partial = PARTIAL_DATA.isOn()

    String response =
        """[
              {
                "version" : "v1",
                "timestamp" : "2014-05-05T00:00:00.000Z",
                "event" : {
                  "color" : "red",
                  "depth" : 9
                }
              }
            ]"""

    String expected =
        """{
            "rows":[{"color|id": "red", "color|desc": "", "dateTime": "2014-05-05 00:00:00.000", "depth": 9}]
        }"""

    def setup() {
        jtb = new JerseyTestBinder(DataServlet.class)
        jtb.druidWebService.jsonResponse = {response}

        Interval interval = new Interval("2014-05-01/2014-06-01")
        AvailabilityTestingUtils.populatePhysicalTableCacheIntervals(jtb, interval)
        PARTIAL_DATA.setOn(true)
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
        PARTIAL_DATA.setOn(partial)
    }

    def "aligned week"() {
        when:
        String result = jtb.getHarness().target("data/shapes/week/color")
            .queryParam("metrics","depth")
            .queryParam("dateTime","2014-05-05%2F2014-05-12")
            .request().get(String.class)

        then: "result should match"
        GroovyTestUtils.compareJson(result, expected)
    }

    def "mis-aligned week"() {
        when:
        jtb.getHarness().target("data/shapes/week/color")
            .queryParam("metrics","depth")
            .queryParam("dateTime","2014-05-06%2F2014-05-13")
            .request()
            .get(String.class)

        then: "result should match"
        thrown BadRequestException
    }

    def "aligned month"() {
        when:
        String result = jtb.getHarness().target("data/shapes/month/color")
            .queryParam("metrics","depth")
            .queryParam("dateTime","2014-05-01%2F2014-06-01")
            .request().get(String.class)

        then: "result should match"
        GroovyTestUtils.compareJson(result, expected)
    }

    def "mis-aligned month"() {
        when:
        jtb.getHarness().target("data/shapes/month/color")
            .queryParam("metrics","depth")
            .queryParam("dateTime","2014-05-02%2F2014-06-02")
            .request()
            .get(String.class)

        then: "api request should fail"
        thrown BadRequestException
    }

    def "missing data"() {
        when:
        String result = jtb.getHarness().target("data/shapes/week/color")
            .queryParam("metrics","depth")
            .queryParam("dateTime","2014-05-26%2F2014-06-02")
            .request().get(String.class)

        then: "result should match"
        result.contains("missingIntervals")
    }

    @IgnoreIf({!DRUID_CACHE.isOn()})
    def "recover if druid cache broken"() {
        setup:
        TestDataCache.cacheEnabled = false

        when:
        String result = jtb.getHarness().target("data/shapes/week/color")
            .queryParam("metrics","depth")
            .queryParam("dateTime","2014-05-05%2F2014-05-12")
            .request().get(String.class)

        then: "result should match"
        GroovyTestUtils.compareJson(result, expected)

        cleanup:
        TestDataCache.cacheEnabled = true
    }
}
