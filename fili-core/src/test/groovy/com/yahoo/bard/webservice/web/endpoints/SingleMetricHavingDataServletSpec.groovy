// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

class SingleMetricHavingDataServletSpec extends BaseDataServletComponentSpec {

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
                "metrics" : ["width"],
                "dateTime": ["2014-06-02%2F2014-06-09"],
                "having": ["width-gt[10]"]
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
                    "width": 11
                },
                {
                    "color|desc": "",
                    "color|id": "Baz",
                    "dateTime": "2014-06-02 00:00:00.000",
                    "width": 12
                }
            ]
        }"""
    }

    @Override
    String getExpectedDruidQuery() {
        """{
                "aggregations": [
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
                    "aggregation": "width",
                    "type": "greaterThan",
                    "value": 10
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
                  "width" : 11
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                  "color" : "Baz",
                  "width" : 12
                }
              }
            ]"""
    }
}
