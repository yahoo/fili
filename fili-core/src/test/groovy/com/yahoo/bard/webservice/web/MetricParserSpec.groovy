// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import org.json.JSONArray

import spock.lang.Specification
import spock.lang.Unroll

class MetricParserSpec extends Specification {

    JSONArray expectedJsonObj1
    JSONArray expectedJsonObj2
    JSONArray expectedJsonObj3

    def setup() {
        expectedJsonObj1 = new JSONArray("[{\"filter\":{\"AND\":\"app3|id-in[mobile,tablet],app2|id-in[abc,xyz]\"},\"name\":\"bcookie\"},{\"filter\":{},\"name\":\"pageviews\"}]")
        expectedJsonObj2 = new JSONArray("[{\"name\":\"pageviews\",\"filter\":{}},{\"name\":\"bcookie\",\"filter\":{}}]")
        expectedJsonObj3 = new JSONArray("[{\"filter\":{\"AND\":\"app3|id-in[mobile,tablet],app2|id-in[abc,xyz]\"},\"name\":\"bcookie\"},{\"filter\":{\"AND\":\"app3|id-in[mobile,tablet],app2|id-in[abc,xyz]\"},\"name\":\"xcookie\"}]")
    }

    def "Test for wrong format of the metric filter with miss alligned brackets"() {
        when:
        MetricParser.generateMetricFilterJsonArray("bcookie(AND(app3|id-in[mobile,tablet]app2|id-in[abc,xyz])").similar(expectedJsonObj1)

        then:
        String expectedMessage = "Metrics parameter values are invalid. The string is: bcookie(AND(app3|id-in[mobile,tablet]app2|id-in[abc,xyz])"
        Exception e = thrown(IllegalArgumentException)
        e.getMessage() == expectedMessage
    }

    def "Validate the json Array Object when metric string is a combination of filtered and non-filtered metrics"() {
        expect:
        MetricParser.generateMetricFilterJsonArray("bcookie(AND(app3|id-in[mobile,tablet],app2|id-in[abc,xyz])),pageviews").similar(expectedJsonObj1)
    }

    def "Validate the json Array Object when metric string contains only non-filtered metrics"() {
        expect:
        MetricParser.generateMetricFilterJsonArray("pageviews,bcookie").similar(expectedJsonObj2)
    }

    def "Validate the json Array Object when metric string contains only filtered metrics"() {
        expect:
        MetricParser.generateMetricFilterJsonArray("bcookie(AND(app3|id-in[mobile,tablet],app2|id-in[abc,xyz])),xcookie(AND(app3|id-in[mobile,tablet],app2|id-in[abc,xyz]))").similar(expectedJsonObj3)
    }

    def "Test for isBracketsBalanced validation"() {
        expect:
        !MetricParser.isBracketsBalanced("bcookie(AND(app3|id-in[mobile,tablet]app2|id-in[abc,xyz]")
        !MetricParser.isBracketsBalanced("bcookie(AND(app3|id-in[mobile,tablet]app2|id-inabc,xyz]))")
        MetricParser.isBracketsBalanced("bcookie,pageviews")
        MetricParser.isBracketsBalanced("bcookie(AND(app3|id-in[mobile,tablet]app2|id-in[abc,xyz])),pageviews")
    }

    @Unroll
    def "Create map using encodeMetricFilters, filter #filter returns values #filteredMetrics"() {
        setup:
        Map<String,String> metricMap = [:]
        MetricParser.encodeMetricFilters(filter, metricMap)

        expect:
        metricMap.values() as List == filteredMetrics

        where:
        filter                                                                               | filteredMetrics
        "bcookie(AND(app3|id-in[mobile,tablet]app2|id-in[abc,xyz])),pageviews"               | ["(AND(app3|id-in[mobile,tablet]app2|id-in[abc,xyz]))"]
        "bcookie(AND(app3|id-in[mobile,tablet]app2|id-in[abc,xyz])),pageviews,fakeMetric"    | ["(AND(app3|id-in[mobile,tablet]app2|id-in[abc,xyz]))"]
        "bcookie(AND(app3|id-in[mobile,tablet]app2|id-in[abc,xyz])),pageviews(garbageField)" | ["(AND(app3|id-in[mobile,tablet]app2|id-in[abc,xyz]))", "(garbageField)"]
        "bcookie,pageviews"                                                                  | []
    }

    @Unroll
    def "Test with #filter create rewritten metrics #metricNames"() {
        expect:
        MetricParser.encodeMetricFilters(filter, [:]) == metricNames

        where:
        filter                                                                               | metricNames
        "bcookie(AND(app3|id-in[mobile,tablet]app2|id-in[abc,xyz])),pageviews"               | "bcookie-7,pageviews"
        "bcookie(AND(app3|id-in[mobile,tablet]app2|id-in[abc,xyz])),pageviews,fakeMetric"    | "bcookie-7,pageviews,fakeMetric"
        "bcookie(AND(app3|id-in[mobile,tablet]app2|id-in[abc,xyz])),pageviews(garbageField)" | "bcookie-7,pageviews-68"
        "bcookie,pageviews"                                                                  | "bcookie,pageviews"
    }
}
