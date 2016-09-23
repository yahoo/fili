// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary

class DruidLimitSpec extends BaseDataServletComponentSpec {
    @Override
    def setup() {
        Dimension dim = Mock(Dimension)
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        dimensionStore.findByApiName("color").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Foo", "FooDesc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Bar", "BarDesc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Baz", "BazDesc"))
         }
    }

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
                "metrics": ["width","height"],
                "dateTime": ["2014-06-02%2F2014-06-30"],
                "count": ["5"]
        ]
    }

    @Override
    String getExpectedDruidQuery() {
        """{
                "queryType": "groupBy",
                "dataSource": {
                    "name": "color_shapes",
                    "type": "table"
                },
                "dimensions": ["color"],
                "aggregations": [
                    {
                        "name": "width",
                        "fieldName": "width",
                        "type": "longSum"
                    },
                    {
                        "name": "height",
                        "fieldName": "height",
                        "type": "longSum"
                    }
                ],
                "postAggregations": []
                ,
                "intervals": ["2014-06-02T00:00:00.000Z/2014-06-30T00:00:00.000Z"],
                "limitSpec": {
                    "type": "default",
                    "limit": 5,
                    "columns": []
                },
                "granularity": ${getTimeGrainString("week")},
                "context": {}
            }"""
    }

    @Override
    String getFakeDruidResponse() {
        """[
              {
                "version" : "v1",
                "timestamp" : "2014-06-10T00:00:00.000Z",
                "event" : {
                  "color" : "Foo",
                  "width" : 10,
                  "height" : 1
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-06-10T00:00:00.000Z",
                "event" : {
                  "color" : "Bar",
                  "width" : 11,
                  "height" : 1
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-06-10T00:00:00.000Z",
                "event" : {
                  "color" : "Baz",
                  "width" : 12,
                  "height" : 1
                }
              }
            ]"""
    }

    @Override
    String getExpectedApiResponse() {
        """{
              "rows" : [
                  {
                    "dateTime" : "2014-06-10 00:00:00.000",
                    "color|id" : "Foo",
                    "color|desc" : "FooDesc",
                    "width" : 10,
                    "height" : 1
                  },
                  {
                    "dateTime" : "2014-06-10 00:00:00.000",
                    "color|id" : "Bar",
                    "color|desc" : "BarDesc",
                    "width" : 11,
                    "height" : 1
                  },
                  {
                    "dateTime" : "2014-06-10 00:00:00.000",
                    "color|id" : "Baz",
                    "color|desc" : "BazDesc",
                    "width" : 12,
                    "height" : 1
                  }
                ]
            }"""
    }
}
