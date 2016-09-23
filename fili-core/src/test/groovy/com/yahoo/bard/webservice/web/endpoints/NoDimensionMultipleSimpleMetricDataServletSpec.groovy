// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

class NoDimensionMultipleSimpleMetricDataServletSpec extends BaseDataServletComponentSpec {

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        return "data/shapes/day/"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["height", "width"],
                "dateTime": ["2014-06-02%2F2014-06-04"]
        ]
    }

    @Override
    String getExpectedDruidQuery() {
        """{
              "queryType" : "timeseries",
              "dataSource" : {
                  "name" : "color_shapes",
                  "type" : "table"
              },
              "aggregations" : [ {
                  "name" : "height",
                  "fieldName" : "height",
                  "type" : "longSum"
              }, {
                  "name" : "width",
                  "fieldName" : "width",
                  "type" : "longSum"
              } ],
              "postAggregations" : [],
              "intervals" : [ "2014-06-02T00:00:00.000Z/2014-06-04T00:00:00.000Z" ],
              "granularity": ${getTimeGrainString()},
              "context": {}
            }"""
    }

    @Override
    String getFakeDruidResponse() {
        """[
              {
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "result" : {
                  "height" : 10,
                  "width" : 12
                }
              },
              {
                "timestamp" : "2014-06-03T00:00:00.000Z",
                "result" : {
                  "height" : 6,
                  "width" : 4
                }
              }
            ]"""
    }

    @Override
    String getExpectedApiResponse() {
        """{
              "rows" : [
                  {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "height" : 10,
                    "width" : 12
                  },
                  {
                    "dateTime" : "2014-06-03 00:00:00.000",
                    "height" : 6,
                    "width" : 4
                  }
                ]
            }"""
    }
}
