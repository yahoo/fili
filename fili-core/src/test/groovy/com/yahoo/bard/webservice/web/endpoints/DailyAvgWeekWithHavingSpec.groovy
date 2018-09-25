// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

// Ensure that having only applies to the outer query when nesting
class DailyAvgWeekWithHavingSpec extends DailyAvgWeekSpec {

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics": ["dayAvgUsers"],
                "dateTime": ["2014-09-01%2F2014-09-08"],
                "having": ["dayAvgUsers-gt[3]"]
        ]
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
                    "dimensions": ["color"],
                    "granularity": ${getTimeGrainString()},
                    "intervals": ["2014-09-01T00:00:00.000Z/2014-09-08T00:00:00.000Z"],
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
            "dimensions": ["color"],
            "granularity": ${getTimeGrainString("week")},
            "having":{"aggregation":"dayAvgUsers","type":"greaterThan","value":3.0},
            "intervals": ["2014-09-01T00:00:00.000Z/2014-09-08T00:00:00.000Z"],
            "postAggregations": [
                {
                    "fields": [
                        {
                            "fieldName": "users_estimate_sum",
                            "type": "fieldAccess"
                        },
                        {
                            "fieldName": "count",
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
}
