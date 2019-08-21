// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection
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

    @Unroll
    def "Remove dateTime sort column from columnDirection map #columnDirection"() {
        expect:
        new TestingDataApiRequestImpl().removeDateTimeSortColumn(columnDirection) == expected

        where:
        columnDirection                                                                          | expected
        ["dateTime":SortDirection.DESC] as Map                                                   | [:]
        ["dateTime":SortDirection.ASC] as Map                                                    | [:]
        ["dateTimexyz":SortDirection.DESC] as Map                                                | ["dateTimexyz":SortDirection.DESC] as Map
        ["xyz":SortDirection.DESC,"dateTime":SortDirection.DESC, "abc":SortDirection.DESC] as Map| ["xyz":SortDirection.DESC,"abc":SortDirection.DESC] as Map
        null                                                                                     | null
        ["dinga":SortDirection.DESC] as Map                                                      | ["dinga":SortDirection.DESC] as Map
    }

    @Unroll
    def "Check dateTime column is first in the sort column map #columnDirection "() {
        expect:
        new TestingDataApiRequestImpl().isDateTimeFirstSortField(columnDirection) == expected

        where:
        columnDirection                                                                          | expected
        ["dateTime":SortDirection.DESC] as Map                                                   | true
        ["dateTime":SortDirection.ASC] as Map                                                    | true
        ["dateTimexyz":SortDirection.DESC] as Map                                                | false
        ["xyz":SortDirection.DESC,"dateTime":SortDirection.DESC, "abc":SortDirection.DESC] as Map| false
        null                                                                                     | false
        ["dinga":SortDirection.DESC] as Map                                                      | false
    }
}
