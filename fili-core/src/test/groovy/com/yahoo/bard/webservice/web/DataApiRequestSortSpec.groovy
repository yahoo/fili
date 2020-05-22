// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.apirequest.utils.TestingDataApiRequestImpl
import com.yahoo.bard.webservice.web.endpoints.DataServlet

import spock.lang.Specification
import spock.lang.Unroll

class DataApiRequestSortSpec extends Specification {

    JerseyTestBinder jtb

    def setup() {
        // Create the test web container to test the resources
        jtb = new JerseyTestBinder(DataServlet.class)
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
    }

    def "Successful execution if dateTime is first field in the sort list"() {
        when:
        javax.ws.rs.core.Response r = jtb.getHarness().target("data/shapes/day/")
                .queryParam("metrics","height")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .queryParam("sort","dateTime|DESC,height|ASC")
                .request().get()

        then:
        r.getStatus() == 200
    }


    def "Successful execution if no dateTime in the sort list"() {
        when:
        javax.ws.rs.core.Response r = jtb.getHarness().target("data/shapes/day/")
                .queryParam("metrics","height")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .queryParam("sort","height|ASC")
                .request().get()

        then:
        r.getStatus() == 200
    }

    def "Successful execution if dateTime is only field in the sort list"() {
        when:
        javax.ws.rs.core.Response r = jtb.getHarness().target("data/shapes/day/")
                .queryParam("metrics","height")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .queryParam("sort","dateTime|DESC")
                .request().get()

        then:
        r.getStatus() == 200
    }

    def "Error if dateTime is not the first sort field in the sort list"() {
        when:
        String expectedMessage = ErrorMessageFormat.DATE_TIME_SORT_VALUE_INVALID.format()
        javax.ws.rs.core.Response r = jtb.getHarness().target("data/shapes/day/")
                .queryParam("metrics","height")
                .queryParam("dateTime","2014-09-01%2F2014-09-10")
                .queryParam("sort","height|ASC,dateTime|DESC")
                .request().get()

        then:
        (String) r.readEntity(String.class).contains(expectedMessage)
        r.getStatus() == 400
    }
}
