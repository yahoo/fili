// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder

import spock.lang.Specification

import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.MultivaluedMap

class UiNonUIBrokerIsolationSpec extends Specification{

    JerseyTestBinder jtb

    def setup() {
        // Create the test web container to test the resources
        jtb = new JerseyTestBinder(DataServlet.class)
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
    }

    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["width"],
                "dateTime": ["2014-05-05%2F2014-05-12"]
        ]
    }

    String makeRequest(MultivaluedMap headers) {
        // Set target of call
        def httpCall = jtb.getHarness().target("data/shapes/week/")
        // Add query params to call
        getQueryParams().each { String key, List<String> values ->
            httpCall = httpCall.queryParam(key, values.join(","))
        }

        // Make the call
        httpCall.request().headers(headers).get(String.class)
    }

    def "check if non-ui request is sent to the non-ui broker"() {
        when:
        makeRequest(null)

        then:
        jtb.nonUiDruidWebService.lastQuery != null
        jtb.uiDruidWebService.lastQuery == null
    }

    def "check if ui request is sent to the ui broker"() {
        when:
        MultivaluedMap headers = new MultivaluedHashMap()
        headers.putSingle("clientid", "UI")
        headers.putSingle("referer", ".")

        makeRequest(headers)

        then:
        jtb.nonUiDruidWebService.lastQuery == null
        jtb.uiDruidWebService.lastQuery != null
    }
}
