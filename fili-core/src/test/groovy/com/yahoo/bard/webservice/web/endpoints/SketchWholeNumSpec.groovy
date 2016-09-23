// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils

import org.joda.time.Interval

class SketchWholeNumSpec extends BaseDataServletComponentSpec {

    @Override
    def setup() {
        DimensionDictionary dimensionDict = jtb.configurationLoader.dimensionDictionary
        dimensionDict.findByApiName("color").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Foo", "FooDesc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Bar", "BarDesc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Baz", "BazDesc"))
        }

        Interval interval = new Interval("2010-01-01/2015-12-31")
        AvailabilityTestingUtils.populatePhysicalTableCacheIntervals(jtb, interval)
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
                "metrics": ["users"],
                "dateTime": ["2014-06-02%2F2014-06-30"]
        ]
    }

    @Override
    String getExpectedDruidQuery() {
        """{
            "aggregations": [
                {
                    "fieldName": "users",
                    "name": "users",
                    "size": 16384,
                    "type": "sketchCount"
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
            "intervals": [
                "2014-06-02T00:00:00.000Z/2014-06-30T00:00:00.000Z"
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
                "timestamp" : "2014-06-10T00:00:00.000Z",
                "event" : {
                  "color" : "Foo",
                  "users" : 1234.56
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-06-10T00:00:00.000Z",
                "event" : {
                  "color" : "Bar",
                  "users" : 987.0234
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-06-10T00:00:00.000Z",
                "event" : {
                  "color" : "Baz",
                  "users" : 9.2
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
                    "users" : 1235
                  },
                  {
                    "dateTime" : "2014-06-10 00:00:00.000",
                    "color|id" : "Bar",
                    "color|desc" : "BarDesc",
                    "users" : 988
                  },
                  {
                    "dateTime" : "2014-06-10 00:00:00.000",
                    "color|id" : "Baz",
                    "color|desc" : "BazDesc",
                    "users" : 10
                  }
                ]
            }"""
    }
}
