// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

class MultipleMetricHavingDataServletSpec extends BaseDataServletComponentSpec {

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        return "data/shapes/week/color"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["width", "height"],
                "dateTime": ["2014-06-02%2F2014-06-09"],
                "having": ["width-gt[10]", "width-lt[12]", "height-eq[50]"]
        ]
    }

    @Override
    String getExpectedApiResponse() {
        """{
            "rows": [
                {
                    "color|desc": "",
                    "color|id": "Bar",
                    "dateTime": "2014-06-02 00:00:00.000",
                    "width": 11,
                    "height": 50
                }
            ]
        }"""
    }

    @Override
    String getExpectedDruidQuery() {
        """{
            "aggregations": [
                {
                    "fieldName": "height",
                    "name": "height",
                    "type": "longSum"
                },
                {
                    "fieldName": "width",
                    "name": "width",
                    "type": "longSum"
                }
            ],
            "dataSource": {
                "name": "color_shapes",
                "type": "table"
            },
            "dimensions": [
                "color"
            ],
            "granularity": ${getTimeGrainString("week")},
            "having": {
                "havingSpecs": [
                    {
                        "aggregation": "height",
                        "type": "equalTo",
                        "value": 50
                    },
                    {
                        "havingSpecs": [
                            {
                                "aggregation": "width",
                                "type": "greaterThan",
                                "value": 10
                            },
                            {
                                "aggregation": "width",
                                "type": "lessThan",
                                "value": 12
                            }
                        ],
                        "type": "and"
                    }
                ],
                "type": "and"
            },
            "intervals": [
                "2014-06-02T00:00:00.000Z/2014-06-09T00:00:00.000Z"
            ],
            "postAggregations": [],
            "queryType": "groupBy",
            "context": {}
        }"""
    }

    @Override
    String getFakeDruidResponse() {
        """[
              {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                  "color" : "Bar",
                  "width" : 11,
                  "height": 50
                }
              }
        ]"""
    }
}
