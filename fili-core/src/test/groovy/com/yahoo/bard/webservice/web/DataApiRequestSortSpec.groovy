// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestImpl
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



    def "If dateTime is not the first value in the sort list, then throw an error"() {
        setup:
        String expectedMessage = ErrorMessageFormat.DATE_TIME_SORT_VALUE_INVALID.format()

        when:
        new DataApiRequestImpl().generateDateTimeSortColumn(["xyz":SortDirection.DESC, "dateTime":SortDirection.DESC])

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage() == expectedMessage
    }

    @Unroll
    def "Validate the sort column and direction map from #sortString string"() {
        expect:
        new DataApiRequestImpl().generateSortColumns(sortString) == expected

        where:
        sortString                        | expected
        "dateTime"                        | ["dateTime":SortDirection.DESC] as Map
        "dateTime|ASC"                    | ["dateTime":SortDirection.ASC] as Map
        "dateTimexyz|DESC"                | ["dateTimexyz":SortDirection.DESC] as Map
        "xyz,dateTime,abc"                | ["xyz":SortDirection.DESC,"dateTime":SortDirection.DESC, "abc":SortDirection.DESC] as Map
        "xyz,dateTime|DESC,abc"           | ["xyz":SortDirection.DESC,"dateTime":SortDirection.DESC, "abc":SortDirection.DESC] as Map
        "xyz|DESC,dateTime|DESC,abc|ASC"  | ["xyz":SortDirection.DESC,"dateTime":SortDirection.DESC, "abc":SortDirection.ASC] as Map
        "xyz|DESC,dateTime,abc|DESC"      | ["xyz":SortDirection.DESC,"dateTime":SortDirection.DESC, "abc":SortDirection.DESC] as Map
        "xyz|DESC,dateTime"               | ["xyz":SortDirection.DESC,"dateTime":SortDirection.DESC] as Map
        ""                                | null
        "dinga"                           | ["dinga":SortDirection.DESC] as Map

    }

    @Unroll
    def "Generate dateTime sort column from columnDirection map #columnDirection"() {
        expect:
        new DataApiRequestImpl().generateDateTimeSortColumn(columnDirection) == expected

        where:
        columnDirection                                                                          | expected
        ["dateTime":SortDirection.DESC] as Map                                                   | Optional.of(new OrderByColumn("dateTime", SortDirection.DESC))
        ["dateTime":SortDirection.ASC] as Map                                                    | Optional.of(new OrderByColumn("dateTime", SortDirection.ASC))
        ["dateTimexyz":SortDirection.DESC] as Map                                                | Optional.empty()
        ["dateTime":SortDirection.DESC, "abc":SortDirection.DESC] as Map                         | Optional.of(new OrderByColumn("dateTime", SortDirection.DESC))
        null                                                                                     | Optional.empty()
        ["dinga":SortDirection.DESC] as Map                                                      | Optional.empty()
    }

    @Unroll
    def "Remove dateTime sort column from columnDirection map #columnDirection"() {
        expect:
        new DataApiRequestImpl().removeDateTimeSortColumn(columnDirection) == expected

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
        new DataApiRequestImpl().isDateTimeFirstSortField(columnDirection) == expected

        where:
        columnDirection                                                                          | expected
        ["dateTime":SortDirection.DESC] as Map                                                   | true
        ["dateTime":SortDirection.ASC] as Map                                                    | true
        ["dateTimexyz":SortDirection.DESC] as Map                                                | false
        ["xyz":SortDirection.DESC,"dateTime":SortDirection.DESC, "abc":SortDirection.DESC] as Map| false
        null                                                                                     | false
        ["dinga":SortDirection.DESC] as Map                                                      | false
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

}
