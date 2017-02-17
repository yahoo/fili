// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection
import com.yahoo.bard.webservice.web.endpoints.DataServlet
import spock.lang.Specification

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

    def "Generate dateTime sort column with ASC value"(){
        when:
        Optional<OrderByColumn> dateTimeSort = new DataApiRequest().generateDateTimeSortColumn("dateTime|asc")

        then:
        dateTimeSort.get().direction == SortDirection.ASC
        dateTimeSort.get().dimension == "dateTime"
    }

    def "check invalid name of dateTime creates error"() {
        setup:
        // mis spelled dateTime as dateTiem.
        String expectedMessage = ErrorMessageFormat.DATE_TIME_SORT_VALUE_INVALID.format()

        when:
        new DataApiRequest().generateDateTimeSortColumn("dateTiem|desc,height|ASC")

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == expectedMessage
    }

    def "Generate dateTime sort direction with default value"() {
        when:
        Optional<OrderByColumn> dateTimeSort = new DataApiRequest().generateDateTimeSortColumn("dateTime")

        then:
        dateTimeSort.get().direction == SortDirection.DESC
        dateTimeSort.get().dimension == "dateTime"
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
}
