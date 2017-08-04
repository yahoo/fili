// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigException
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.druid.client.DruidServiceConfig
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService
import com.yahoo.bard.webservice.util.GroovyTestUtils

import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

import javax.ws.rs.core.Response

@Timeout(30)    // Fail test if hangs
class DataServletContextsSpec extends Specification {

    private static final SystemConfig systemConfig = SystemConfigProvider.getInstance()

    private static final String DRUID_URL_SETTING = systemConfig.getPackageVariableName("non_ui_druid_broker")

    static String saveDruidURL

    JerseyTestBinder jtb

    DruidWebService testWebService
    DruidWebService testMetadataWebService

    String expectedDruidQueryFormat =
    """
            {
                "queryType" : "groupBy",
                "dataSource" : {
                    "name" : "color_shapes",
                    "type" : "table"
                },
                "dimensions" : [ "color" ],
                "aggregations" : [ {
                    "name" : "height",
                    "fieldName" : "height",
                    "type" : "longSum"
                } ],
                "postAggregations" : [ ],
                "intervals" : [ "2014-09-01T00:00:00.000Z/2014-09-08T00:00:00.000Z" ],
                "granularity": ${BaseDataServletComponentSpec.getTimeGrainString("week")},
                "context": {}
                %s
            }
        """

    // Unused, but specified to control the ultimate response
    String defaultResponse = """[
              {
                "version" : "v1",
                "timestamp" : "2014-09-01T00:00:00.000Z",
                "event" : {
                  "color" : "Foo",
                  "height" : 10
                }
              }
            ]"""

    def setupSpec() {
        // Intercept Druid requests with StubDruidServlet
        try {
            saveDruidURL = systemConfig.getStringProperty(DRUID_URL_SETTING)
        } catch (SystemConfigException ignored) {
            saveDruidURL = null
        }
        if (saveDruidURL == null) {
            systemConfig.setProperty(DRUID_URL_SETTING,"http://localhost:9998/druid")
        }
    }

    def setup() {
        // Create the test web container to test the resources
        ArrayList<Class>  resources = [DataServlet.class]
        if (saveDruidURL == null) {
            resources.add(TestDruidServlet.class)
        }
        testWebService = new TestDruidWebService("default web service");
        testMetadataWebService = new TestDruidWebService("default metadata web service");
        jtb = new TestWebserviceJerseyTestBinder((Class[]) resources)
        assert jtb.druidWebService instanceof DruidWebService
        TestDruidServlet.setResponse(defaultResponse)
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
    }

    def cleanupSpec() {
        systemConfig.getProperties().remove(DRUID_URL_SETTING)
        if (saveDruidURL == null) {
            systemConfig.getProperties().remove(DRUID_URL_SETTING)
        } else {
            systemConfig.setProperty(DRUID_URL_SETTING,saveDruidURL)
        }
    }

    @Unroll
    def "Request with values"() {
        when:
        testWebService.serviceConfig = new DruidServiceConfig("Ui/Non-Ui Broker", null, timeout, priority);

        TestDruidServlet.setResponse(defaultResponse)

        String expectedDruidQuery = String.format(expectedDruidQueryFormat, text)

        then:
        Response r = jtb.getHarness().target("data/shapes/week/color")
                .queryParam("metrics","height")
                .queryParam("dateTime","2014-09-01%2F2014-09-08")
                .request().get()
        r.getStatus() == 200
        GroovyTestUtils.compareJson(testWebService.jsonQuery, expectedDruidQuery)

        where:
        timeout | priority | text
        null    | null     | """ """
        123     | null     | """, "context": { "timeout": 123 } """
        123     | 4        | """, "context": { "timeout": 123, "priority" : 4 } """
        null    | 4        | """, "context": { "priority" : 4 } """
    }

    public class TestWebserviceJerseyTestBinder extends JerseyTestBinder {
        public TestWebserviceJerseyTestBinder(java.lang.Class<?>... resourceClasses) {
            super(true, resourceClasses);
        }

        @Override
        void buildWebServices() {
            state.uiWebService = testWebService
            state.nonUiWebService = testWebService
            state.metadataWebService = testMetadataWebService
        }
    }
}
