// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import spock.lang.Timeout

/**
 * Checks that all of the table/grain pairs have the dimensions and columns expected.
 */
@Timeout(30)    // Fail test if hangs
class ExpectedTableEndpointSpec extends BaseTableServletComponentSpec {

    @Override
    Class<?>[] getResourceClasses() {
        [TablesServlet.class]
    }

    @Override
    String getTarget() {
        return "tables/shapes"
    }

    @Override
    String getExpectedApiResponse() {
        """{
                "rows" : [
                    {
                          "category": "General",
                          "name": "shapes",
                          "longName": "shapes",
                          "granularity": "all",
                          "uri": "http://localhost:9998/tables/shapes/all"
                    },{
                          "category": "General",
                          "longName": "shapes",
                          "name": "shapes",
                          "granularity": "day",
                          "uri": "http://localhost:9998/tables/shapes/day"
                    },{
                          "category": "General",
                          "name": "shapes",
                          "longName": "shapes",
                          "granularity": "week",
                          "uri": "http://localhost:9998/tables/shapes/week"
                    },{
                          "category": "General",
                          "name": "shapes",
                          "longName": "shapes",
                          "granularity": "month",
                          "uri": "http://localhost:9998/tables/shapes/month"
                    }
                 ]
           }"""
    }
}
