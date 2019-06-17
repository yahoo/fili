// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE
import static com.yahoo.bard.webservice.config.BardFeatureFlag.PARTIAL_DATA

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.config.BardFeatureFlag
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.cache.TestDataCache
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils
import com.yahoo.bard.webservice.util.DateTimeFormatterFactory
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.web.endpoints.DataServlet

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode

import org.joda.time.Interval

import spock.lang.IgnoreIf
import spock.lang.Specification
import spock.lang.Unroll

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


    String dateTimeFormatKey = "bard__output_datetime_format"

    def setup() {
        jtb = new JerseyTestBinder(DataServlet.class)
        jtb.druidWebService.jsonResponse = {response}

        Interval interval = new Interval("2014-05-01/2014-06-01")
        AvailabilityTestingUtils.populatePhysicalTableCacheIntervals(jtb, interval)
        PARTIAL_DATA.setOn(true)

        SystemConfigProvider.instance.setProperty(dateTimeFormatKey, "yyyy-MM-dd")
        DateTimeFormatterFactory.datetimeOutputFormatter = null
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
        PARTIAL_DATA.setOn(partial)
        SystemConfigProvider.instance.clearProperty(dateTimeFormatKey)
        DateTimeFormatterFactory.datetimeOutputFormatter = null

    }

    @Unroll
    def "missing data at #grain requested #requestedIntervals expecting #expectedIntervals"() {
        when:
        String result = jtb.getHarness().target("data/shapes/" + grain + "/color")
            .queryParam("metrics","depth")
            .queryParam("dateTime",requestedIntervals)
            .request().get(String.class)

        JsonNode resultJson = (new ObjectMappersSuite()).getMapper().readTree(result)
        JsonNode missingIntervals = resultJson.get("meta").get("missingIntervals")

        then: "result should match"
        result.contains("missingIntervals")
        missingIntervals != null
        ((ArrayNode) missingIntervals).get(0).asText() == expectedIntervals
        where:
        grain  | requestedIntervals      | expectedIntervals
        "week" | "2014-05-26/2014-06-02" | "2014-05-26/2014-06-02"
        "day"  | "2014-05-26/2014-06-02" | "2014-06-01/2014-06-02"
    }

    @Unroll
    def "missing data is published if any partial data flags are true"() {
        when:
        BardFeatureFlag.PARTIAL_DATA.setOn(partial)
        BardFeatureFlag.PARTIAL_DATA_PROTECTION.setOn(protection)
        BardFeatureFlag.PARTIAL_DATA_QUERY_OPTIMIZATION.setOn(query)

        String requestedIntervals = "2014-05-26/2014-06-02"
        String expectedIntervals = requestedIntervals

        String result = jtb.getHarness().target("data/shapes/week/color")
                .queryParam("metrics","depth")
                .queryParam("dateTime",requestedIntervals)
                .request().get(String.class)

        JsonNode resultJson = (new ObjectMappersSuite()).getMapper().readTree(result)
        boolean hasMissing = resultJson.get("meta") != null && resultJson.get("meta").get("missingIntervals") != null
        JsonNode metaNode = resultJson.get("meta")


        then: "result should match"
        hasMissing == expectMissing

        cleanup:
        BardFeatureFlag.PARTIAL_DATA.reset()
        BardFeatureFlag.PARTIAL_DATA_QUERY_OPTIMIZATION.reset()
        BardFeatureFlag.PARTIAL_DATA_PROTECTION.reset()

        where:
        partial | protection | query | expectMissing
        true  | true  | true  | true
        true  | true  | false | true
        true  | false | true  | true
        true  | false | false | true
        false | true  | true  | true
        false | true  | false | true
        false | false | true  | true
        false | false | false | false
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
