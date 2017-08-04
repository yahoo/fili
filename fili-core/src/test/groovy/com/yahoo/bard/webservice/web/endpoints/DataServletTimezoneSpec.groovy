// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService
import com.yahoo.bard.webservice.util.GroovyTestUtils

import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

import javax.ws.rs.core.Response

@Timeout(30)
// Fail test if hangs
class DataServletTimezoneSpec extends Specification {

    static SystemConfig systemConfig = SystemConfigProvider.getInstance()

    private static final String DRUID_URL_SETTING = systemConfig.getPackageVariableName("druid_broker")

    static String contextKey = systemConfig.getPackageVariableName("time_context_implementation")

    static String originalContextPropertyValue

    final static String STANDARD_CONTEXT_CLASS_NAME = StandardGranularityParser.getCanonicalName()

    JerseyTestBinder jtb

    DruidWebService testWebService
    DruidWebService testMetadataWebService

    static String systemTimeZone
    static String timeZonePropertyName = systemConfig.getPackageVariableName("timezone")

    String expectedDruidQueryFormat =
            """{
                "queryType" : "groupBy",
                "dataSource" : {
                    "name" : "hourly",
                    "type" : "table"
                },
                "dimensions" : [ {"dimension":"misc","outputName":"other","type":"default"} ],
                "aggregations" : [ {
                    "name" : "limbs",
                    "fieldName" : "limbs",
                    "type" : "longSum"
                } ],
                "postAggregations" : [ ],
                "intervals" : [ "%s" ],
                "granularity" : {
                    "type" : "period",
                    "period" : "PT1H"%s
                },
                "context": {}
            }"""

    def setupSpec() {
        // Intercept Druid requests with StubDruidServlet
        System.setProperty(DRUID_URL_SETTING, "http://localhost:9998/druid")
        systemTimeZone = systemConfig.getStringProperty(timeZonePropertyName, "UTC")
        originalContextPropertyValue = System.getProperty(contextKey)
    }

    def cleanup() {
        // Release the test web container
        systemConfig.setProperty(timeZonePropertyName, systemTimeZone)
        if (jtb != null) {
            jtb.tearDown()
        }
        if (originalContextPropertyValue == null) {
            System.clearProperty(contextKey)
        } else {
            System.setProperty(contextKey, originalContextPropertyValue)
        }
    }

    def cleanupSpec() {
        System.getProperties().remove(DRUID_URL_SETTING)
    }

    def startJerseyTestBinder() {
        ArrayList<Class<?>> resources = [DataServlet.class, TestDruidServlet.class]
        testWebService = new TestDruidWebService("default web service")
        testMetadataWebService = new TestDruidWebService("default metadata web service")
        jtb = new TestWebserviceJerseyTestBinder((Class<?>[]) resources.toArray())
        assert jtb.druidWebService instanceof DruidWebService
    }

    @Unroll
    def "Request sets time zone and adjusts query date '#timeZone'"() {
        setup:
        systemConfig.setProperty(systemConfig.getPackageVariableName("timezone"), "America/Los_Angeles")
        System.setProperty(contextKey, STANDARD_CONTEXT_CLASS_NAME)
        String expectedDruidQuery = String.format(expectedDruidQueryFormat, expectedDate, expectedTimezone)

        startJerseyTestBinder()

        when:
        Response r = jtb.getHarness().target("data/hourly/hour/other")
                .queryParam("metrics", "limbs")
                .queryParam("dateTime", queryDate)
                .queryParam("timeZone", timeZone)
                .request().get()

        then:
        r.getStatus() == 200
        GroovyTestUtils.compareJson(testWebService.jsonQuery, expectedDruidQuery)

        //@formatter:off
        where:
        timeZone              | queryDate                                             | expectedDate                                                      | expectedTimezone
        "UTC"                 | "2014-09-01T00:00:00.000%2F2014-09-01T02:00:00.000"   | "2014-09-01T00:00:00.000Z/2014-09-01T02:00:00.000Z"               | ', "timeZone": "UTC" '
        "America/Chicago"     | "2014-09-01T00:00:00.000%2F2014-09-01T02:00:00.000"   | "2014-09-01T00:00:00.000-05:00/2014-09-01T02:00:00.000-05:00"     | ', "timeZone": "America/Chicago" '
        "America/Los_Angeles" | "2014-09-01T00:00:00.000%2F2014-09-01T02:00:00.000"   | "2014-09-01T00:00:00.000-07:00/2014-09-01T02:00:00.000-07:00"     | ', "timeZone": "America/Los_Angeles" '
        //@formatter:on
    }

    @Unroll
    def "Without request timezone, query uses system time zone '#timeZone'"() {
        setup:
        System.setProperty(contextKey, STANDARD_CONTEXT_CLASS_NAME)
        systemConfig.setProperty(systemConfig.getPackageVariableName("timezone"), timeZone)
        startJerseyTestBinder()
        String expectedDruidQuery = String.format(expectedDruidQueryFormat, expectedDate, expectedTimezone)

        when:
        Response r = jtb.getHarness().target("data/hourly/hour/other")
                .queryParam("metrics", "limbs")
                .queryParam("dateTime", queryDate)
                .request().get()

        then:
        r.getStatus() == 200
        GroovyTestUtils.compareJson(testWebService.jsonQuery, expectedDruidQuery)

        //@formatter:off
        where:
        timeZone              | queryDate                                             | expectedDate                                                      | expectedTimezone
        "UTC"                 | "2014-09-01T00:00:00.000%2F2014-09-01T02:00:00.000"   | "2014-09-01T00:00:00.000Z/2014-09-01T02:00:00.000Z"               | """, "timeZone": "UTC" """
        "America/Chicago"     | "2014-09-01T00:00:00.000%2F2014-09-01T02:00:00.000"   | "2014-09-01T00:00:00.000-05:00/2014-09-01T02:00:00.000-05:00"     | """, "timeZone": "America/Chicago" """
        "America/Los_Angeles" | "2014-09-01T00:00:00.000%2F2014-09-01T02:00:00.000"   | "2014-09-01T00:00:00.000-07:00/2014-09-01T02:00:00.000-07:00"     | """, "timeZone": "America/Los_Angeles" """
        //@formatter:on
    }

    public class TestWebserviceJerseyTestBinder extends JerseyTestBinder {
        public TestWebserviceJerseyTestBinder(java.lang.Class<?>... resourceClasses) {
            super(true, resourceClasses)
        }

        @Override
        void buildWebServices() {
            state.webService = testWebService
            state.metadataWebService = testWebService
        }
    }
}
