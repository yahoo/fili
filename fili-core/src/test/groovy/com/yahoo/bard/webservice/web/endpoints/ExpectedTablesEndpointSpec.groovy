// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import spock.lang.Timeout

/**
 * Checks that all of the table/grain pairs have the dimensions and columns expected.
 */
@Timeout(30)    // Fail test if hangs
class ExpectedTablesEndpointSpec extends BaseTableServletComponentSpec {

    @Override
    Class<?>[] getResourceClasses() {
        [TablesServlet.class]
    }

    @Override
    String getTarget() {
        return "tables"
    }

    @Override
    String getExpectedApiResponse() {
        """{
            "rows": [
                {
                    "category": "General",
                    "longName": "hourly",
                    "name": "hourly",
                    "granularity": "all",
                    "uri": "http://localhost:9998/tables/hourly/all"
                }, {
                    "category": "General",
                    "longName": "hourly",
                    "name": "hourly",
                    "granularity": "day",
                    "uri": "http://localhost:9998/tables/hourly/day"
                }, {
                    "category": "General",
                    "longName": "hourly",
                    "name": "hourly",
                    "granularity": "hour",
                    "uri": "http://localhost:9998/tables/hourly/hour"
                }, {
                    "category": "General",
                    "longName": "hourly",
                    "name": "hourly",
                    "granularity": "month",
                    "uri": "http://localhost:9998/tables/hourly/month"
                }, {
                    "category": "General",
                    "longName": "hourly",
                    "name": "hourly",
                    "granularity": "week",
                    "uri": "http://localhost:9998/tables/hourly/week"
                }, {
                    "category": "General",
                    "longName": "hourly_monthly",
                    "name": "hourly_monthly",
                    "granularity": "all",
                    "uri": "http://localhost:9998/tables/hourly_monthly/all"
                }, {
                    "category": "General",
                    "longName": "hourly_monthly",
                    "name": "hourly_monthly",
                    "granularity": "day",
                    "uri": "http://localhost:9998/tables/hourly_monthly/day"
                }, {
                    "category": "General",
                    "longName": "hourly_monthly",
                    "name": "hourly_monthly",
                    "granularity": "hour",
                    "uri": "http://localhost:9998/tables/hourly_monthly/hour"
                }, {
                    "category": "General",
                    "longName": "hourly_monthly",
                    "name": "hourly_monthly",
                    "granularity": "month",
                    "uri": "http://localhost:9998/tables/hourly_monthly/month"
                }, {
                    "category": "General",
                    "longName": "hourly_monthly",
                    "name": "hourly_monthly",
                    "granularity": "week",
                    "uri": "http://localhost:9998/tables/hourly_monthly/week"
                }, {
                    "category": "General",
                    "longName": "monthly",
                    "name": "monthly",
                    "granularity": "all",
                    "uri": "http://localhost:9998/tables/monthly/all"
                }, {
                    "category": "General",
                    "longName": "monthly",
                    "name": "monthly",
                    "granularity": "day",
                    "uri": "http://localhost:9998/tables/monthly/day"
                }, {
                    "category": "General",
                    "longName": "monthly",
                    "name": "monthly",
                    "granularity": "month",
                    "uri": "http://localhost:9998/tables/monthly/month"
                }, {
                    "category": "General",
                    "longName": "monthly",
                    "name": "monthly",
                    "granularity": "week",
                    "uri": "http://localhost:9998/tables/monthly/week"
                }, {
                    "category": "General",
                    "longName": "pets",
                    "name": "pets",
                    "granularity": "all",
                    "uri": "http://localhost:9998/tables/pets/all"
                }, {
                    "category": "General",
                    "longName": "pets",
                    "name": "pets",
                    "granularity": "day",
                    "uri": "http://localhost:9998/tables/pets/day"
                }, {
                    "category": "General",
                    "longName": "pets",
                    "name": "pets",
                    "granularity": "month",
                    "uri": "http://localhost:9998/tables/pets/month"
                }, {
                    "category": "General",
                    "longName": "pets",
                    "name": "pets",
                    "granularity": "week",
                    "uri": "http://localhost:9998/tables/pets/week"
                }, {
                    "category": "General",
                    "longName": "shapes",
                    "name": "shapes",
                    "granularity": "all",
                    "uri": "http://localhost:9998/tables/shapes/all"
                }, {
                    "category": "General",
                    "longName": "shapes",
                    "name": "shapes",
                    "granularity": "day",
                    "uri": "http://localhost:9998/tables/shapes/day"
                }, {
                    "category": "General",
                    "longName": "shapes",
                    "name": "shapes",
                    "granularity": "month",
                    "uri": "http://localhost:9998/tables/shapes/month"
                }, {
                    "category": "General",
                    "longName": "shapes",
                    "name": "shapes",
                    "granularity": "week",
                    "uri": "http://localhost:9998/tables/shapes/week"
                }
            ]
        }"""
    }
}
