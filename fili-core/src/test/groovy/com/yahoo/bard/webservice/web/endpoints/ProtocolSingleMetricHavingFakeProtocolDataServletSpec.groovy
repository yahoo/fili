// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

class ProtocolSingleMetricHavingFakeProtocolDataServletSpec extends BaseDataServletComponentSpec {

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
                "metrics" : ["width(fakeProtocol=dayAvg)"],
                "dateTime": ["2014-06-02%2F2014-06-09"],
                "having": ["width(fakeProtocol=dayAvg)-gt[10]"]
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
                    "width(fakeProtocol=dayAvg)": 11
                },
                {
                    "color|desc": "",
                    "color|id": "Baz",
                    "dateTime": "2014-06-02 00:00:00.000",
                    "width(fakeProtocol=dayAvg)": 12
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
                        "name": "width(fakeProtocol=dayAvg)",
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
                    "aggregation": "width(fakeProtocol=dayAvg)",
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
                  "width(fakeProtocol=dayAvg)" : 11
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                  "color" : "Baz",
                  "width(fakeProtocol=dayAvg)" : 12
                }
              }
            ]"""
    }
}
