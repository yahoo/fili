// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

class ProtocolSingleMetricHavingReaggProtocolDataServletSpec extends BaseDataServletComponentSpec {

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
                "metrics" : ["width(reagg=dayAvg)"],
                "dateTime": ["2014-06-02%2F2014-06-09"],
                "having": ["width(reagg=dayAvg)-gt[10]"]
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
                    "width(reagg=dayAvg)": 11
                },
                {
                    "color|desc": "",
                    "color|id": "Baz",
                    "dateTime": "2014-06-02 00:00:00.000",
                    "width(reagg=dayAvg)": 12
                }
            ]
        }"""
    }

    @Override
    String getExpectedDruidQuery() {
        """{
        "aggregations": [{
                             "fieldName": "one",
                             "name": "count",
                             "type": "longSum"
                         }, {
                             "fieldName": "width",
                             "name": "width",
                             "type": "longSum"
                         }],
        "context": {},
        "dataSource": {
        "query": {
            "aggregations": [{
                                 "fieldName": "width",
                                 "name": "width",
                                 "type": "longSum"
                             }],
            "context": {},
            "dataSource": {
                "name": "color_shapes",
                "type": "table"
            },
            "dimensions": ["color"],
            "granularity": {
                "period": "P1D",
                "timeZone": "UTC",
                "type": "period"
            },
            "intervals": ["2014-06-02T00:00:00.000Z/2014-06-09T00:00:00.000Z"],
            "postAggregations": [{
                                     "name": "one",
                                     "type": "constant",
                                     "value": 1.0
                                 }],
            "queryType": "groupBy"
        },
        "type": "query"
    },
        "dimensions": ["color"],
        "granularity": {
        "period": "P1W",
        "timeZone": "UTC",
        "type": "period"
    },
        "having": {
        "aggregation": "width(reagg=dayAvg)",
        "type": "greaterThan",
        "value": 10.0
    },
        "intervals": ["2014-06-02T00:00:00.000Z/2014-06-09T00:00:00.000Z"],
        "postAggregations": [{
                                 "fields": [{
                                                "fieldName": "count",
                                                "type": "fieldAccess"
                                            }, {
                                                "fieldName": "width",
                                                "type": "fieldAccess"
                                            }],
                                 "fn": "/",
                                 "name": "width(reagg=dayAvg)",
                                 "type": "arithmetic"
                             }],
        "queryType": "groupBy"
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
                  "width(reagg=dayAvg)" : 11
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                  "color" : "Baz",
                  "width(reagg=dayAvg)" : 12
                }
              }
            ]"""
    }
}
