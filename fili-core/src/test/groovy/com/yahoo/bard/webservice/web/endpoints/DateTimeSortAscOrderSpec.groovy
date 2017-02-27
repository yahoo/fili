// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary

class DateTimeSortAscOrderSpec extends BaseDataServletComponentSpec {
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
                "metrics": ["width","height"],
                "dateTime": ["2014-06-02%2F2014-06-30"],
                "sort": ["dateTime|ASC,width|DESC"]
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
                "postAggregations": [],
                "intervals": ["2014-06-02T00:00:00.000Z/2014-06-30T00:00:00.000Z"],
                 "limitSpec": {
                "columns": [
                  {
                    "dimension": "width",
                    "direction": "DESC"
                  }
                ],
                "type": "default"
              },
                "granularity": ${getTimeGrainString("week")},
                "context": {}
            }"""
    }

    @Override
    String getFakeDruidResponse() {
        """[
              {
                "version": "v1",
                "timestamp": "2014-06-08T00:00:00.000Z",
                "event": {
                  "color": "Bar",
                  "width": 108,
                  "height": 81
                }
              },
              {
                "version": "v1",
                "timestamp": "2014-06-08T00:00:00.000Z",
                "event": {
                  "color": "Foo",
                  "width": 208,
                  "height": 82
                }
              },
              {
                "version": "v1",
                "timestamp": "2014-06-11T00:00:00.000Z",
                "event": {
                  "color": "Bar",
                  "width": 111,
                  "height": 111
                }
              },
              {
                "version": "v1",
                "timestamp": "2014-06-11T00:00:00.000Z",
                "event": {
                  "color": "Foo",
                  "width": 211,
                  "height": 112
                }
              },
               {
                "version": "v1",
                "timestamp": "2014-06-10T00:00:00.000Z",
                "event": {
                  "color": "Foo",
                  "width": 110,
                  "height": 101
                }
              },
              {
                "version": "v1",
                "timestamp": "2014-06-10T00:00:00.000Z",
                "event": {
                  "color": "Bar",
                  "width": 210,
                  "height": 102
                }
              },
              {
                "version": "v1",
                "timestamp": "2014-06-12T00:00:00.000Z",
                "event": {
                  "color": "Baz",
                  "width": 112,
                  "height": 121
                }
              },
              {
                "version": "v1",
                "timestamp": "2014-06-12T00:00:00.000Z",
                "event": {
                  "color": "Foo",
                  "width": 212,
                  "height": 122
                }
              },
              {
                "version": "v1",
                "timestamp": "2014-06-09T00:00:00.000Z",
                "event": {
                  "color": "Bar",
                  "width": 19,
                  "height": 91
                }
              },
              {
                "version": "v1",
                "timestamp": "2014-06-09T00:00:00.000Z",
                "event": {
                  "color": "Foo",
                  "width": 29,
                  "height": 92
                }
              }
        ]"""
    }

    @Override
    String getExpectedApiResponse() {
        """{
              "rows": [
                {
                  "color|desc": "BarDesc",
                  "color|id": "Bar",
                  "dateTime": "2014-06-08 00:00:00.000",
                  "height": 81,
                  "width": 108
                },
                {
                  "color|desc": "FooDesc",
                  "color|id": "Foo",
                  "dateTime": "2014-06-08 00:00:00.000",
                  "height": 82,
                  "width": 208
                },
                {
                  "color|desc": "BarDesc",
                  "color|id": "Bar",
                  "dateTime": "2014-06-09 00:00:00.000",
                  "height": 91,
                  "width": 19
                },
                {
                  "color|desc": "FooDesc",
                  "color|id": "Foo",
                  "dateTime": "2014-06-09 00:00:00.000",
                  "height": 92,
                  "width": 29
                },
                {
                  "color|desc": "FooDesc",
                  "color|id": "Foo",
                  "dateTime": "2014-06-10 00:00:00.000",
                  "height": 101,
                  "width": 110
                },
                {
                  "color|desc": "BarDesc",
                  "color|id": "Bar",
                  "dateTime": "2014-06-10 00:00:00.000",
                  "height": 102,
                  "width": 210
                },
                {
                  "color|desc": "BarDesc",
                  "color|id": "Bar",
                  "dateTime": "2014-06-11 00:00:00.000",
                  "height": 111,
                  "width": 111
                },
                {
                  "color|desc": "FooDesc",
                  "color|id": "Foo",
                  "dateTime": "2014-06-11 00:00:00.000",
                  "height": 112,
                  "width": 211
                },
                {
                  "color|desc": "BazDesc",
                  "color|id": "Baz",
                  "dateTime": "2014-06-12 00:00:00.000",
                  "height": 121,
                  "width": 112
                },
                {
                  "color|desc": "FooDesc",
                  "color|id": "Foo",
                  "dateTime": "2014-06-12 00:00:00.000",
                  "height": 122,
                  "width": 212
                }
              ]
            }"""
    }
}
