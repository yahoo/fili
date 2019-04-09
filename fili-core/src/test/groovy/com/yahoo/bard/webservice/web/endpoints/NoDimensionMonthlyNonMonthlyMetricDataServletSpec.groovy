// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

/**
 * This test is checking that a "total" and a "daily average" metric, when asked for together, will resolve to a proper
 * physical table. In particular, it will resolve to a daily table, rather than a monthly table.
 */
class NoDimensionMonthlyNonMonthlyMetricDataServletSpec extends BaseDataServletComponentSpec {

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        return "data/shapes/month/"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["users", "dayAvgUsers"],
                "dateTime": ["2014-06-01%2F2014-07-01"]
        ]
    }

    @Override
    String getExpectedApiResponse() {
        """{
            "rows" : [
                {
                    "dateTime" : "2014-06-01 00:00:00.000",
                    "users" : 75,
                    "dayAvgUsers" : 2.5
                }
            ]
        }"""
    }

    @Override
    String getExpectedDruidQuery() {
        """{
            "aggregations": [
                {
                    "fieldName": "one",
                    "name": "count",
                    "type": "longSum"
                },
                {
                    "fieldName": "users",
                    "name": "users",
                    "size": 16384,
                    "type": "thetaSketch"
                },
                {
                    "fieldName": "users_estimate",
                    "name": "users_estimate_sum",
                    "type": "doubleSum"
                }
            ],
            "dataSource": {
                "query": {
                    "aggregations": [
                        {
                            "fieldName": "users",
                            "name": "users",
                            "size": 16384,
                            "type": "thetaSketch"
                        }
                    ],
                    "dataSource": {
                        "name": "color_shapes",
                        "type": "table"
                    },
                    "dimensions": [],
                    "granularity": ${getTimeGrainString()},
                    "intervals": [
                        "2014-06-01T00:00:00.000Z/2014-07-01T00:00:00.000Z"
                    ],
                    "postAggregations": [
                        {
                            "field": {
                                "fieldName": "users",
                                "type": "fieldAccess"
                            },
                            "name": "users_estimate",
                            "type": "thetaSketchEstimate"
                        },
                        {
                            "name": "one",
                            "type": "constant",
                            "value": 1.0
                        }
                    ],
                    "queryType": "groupBy",
                    "context": {}
                },
                "type": "query"
            },
            "dimensions": [],
            "granularity": ${getTimeGrainString("month")},
            "intervals": [
                "2014-06-01T00:00:00.000Z/2014-07-01T00:00:00.000Z"
            ],
            "postAggregations": [
                {
                    "fields": [
                        {
                            "fieldName": "count",
                            "type": "fieldAccess"
                        },
                        {
                            "fieldName": "users_estimate_sum",
                            "type": "fieldAccess"
                        }
                    ],
                    "fn": "/",
                    "name": "dayAvgUsers",
                    "type": "arithmetic"
                }
            ],
            "queryType": "groupBy",
            "context": {}
        }"""
    }

    @Override
    String getFakeDruidResponse() {
        """[
            {
                "version" : "v1",
                "timestamp" : "2014-06-01T00:00:00.000Z",
                "event" : {
                    "users" : 75,
                    "dayAvgUsers": 2.5
                }
            }
        ]"""
    }
}
