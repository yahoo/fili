// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary


class DruidOrderBySpec extends BaseDataServletComponentSpec {
    @Override
    def setup() {
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
                "metrics": ["width","depth"],
                "dateTime": ["2014-06-02%2F2014-06-30"],
                "sort": ["width|desc","depth|asc"]
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
                        "name": "depth",
                        "fieldName": "depth",
                        "type": "longSum"
                    }
                ],
                "intervals": ["2014-06-02T00:00:00.000Z/2014-06-30T00:00:00.000Z"],
                "limitSpec": {
                    "type": "default",
                    "columns": [
                        {
                            "dimension": "width",
                            "direction": "DESC"
                        },
                        {
                            "dimension": "depth",
                            "direction": "ASC"
                        }
                    ]
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
                  "depth" : 1
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-06-10T00:00:00.000Z",
                "event" : {
                  "color" : "Bar",
                  "width" : 11,
                  "depth" : 1
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-06-10T00:00:00.000Z",
                "event" : {
                  "color" : "Baz",
                  "width" : 12,
                  "depth" : 1
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
                    "depth" : 1
                  },
                  {
                    "dateTime" : "2014-06-10 00:00:00.000",
                    "color|id" : "Bar",
                    "color|desc" : "BarDesc",
                    "width" : 11,
                    "depth" : 1
                  },
                  {
                    "dateTime" : "2014-06-10 00:00:00.000",
                    "color|id" : "Baz",
                    "color|desc" : "BazDesc",
                    "width" : 12,
                    "depth" : 1
                  }
                ]
            }"""
    }
}
