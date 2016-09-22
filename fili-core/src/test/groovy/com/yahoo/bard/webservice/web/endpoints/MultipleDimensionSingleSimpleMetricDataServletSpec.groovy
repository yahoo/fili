// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary

class MultipleDimensionSingleSimpleMetricDataServletSpec extends BaseDataServletComponentSpec {

    @Override
    def setup() {
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        dimensionStore.findByApiName("color").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "color1", "color1Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "color2", "color2Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "color3", "color3Desc"))
        }
        dimensionStore.findByApiName("shape").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "shape1", "shape1Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "shape2", "shape2Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "shape3", "shape3Desc"))
        }
        dimensionStore.findByApiName("size").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "size1", "size1Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "size2", "size2Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "size3", "size3Desc"))
        }
    }

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        return "data/shapes/day/color/shape/size"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["width"],
                "dateTime": ["2014-05-01%2F2014-05-02"]
        ]
    }

    @Override
    String getExpectedApiResponse() {
        """{
              "rows" : [
                  {
                    "dateTime" : "2014-05-01 00:00:00.000",
                    "color|id" : "color1",
                    "color|desc" : "color1Desc",
                    "shape|id" : "shape1",
                    "shape|desc" : "shape1Desc",
                    "size|id" : "size1",
                    "size|desc" : "size1Desc",
                    "width" : 111
                  },
                  {
                    "dateTime" : "2014-05-01 00:00:00.000",
                    "color|id" : "color2",
                    "color|desc" : "color2Desc",
                    "shape|id" : "shape2",
                    "shape|desc" : "shape2Desc",
                    "size|id" : "size2",
                    "size|desc" : "size2Desc",
                    "width" : 222
                  },
                  {
                    "dateTime" : "2014-05-01 00:00:00.000",
                    "color|id" : "color3",
                    "color|desc" : "color3Desc",
                    "shape|id" : "shape3",
                    "shape|desc" : "shape3Desc",
                    "size|id" : "size3",
                    "size|desc" : "size3Desc",
                    "width" : 333
                  }
                ]
            }"""
    }

    @Override
    String getExpectedDruidQuery() {
        """{
              "queryType": "groupBy",
              "granularity": ${timeGrainString},
              "dimensions": ["color","shape","size"],
              "intervals": [ "2014-05-01T00:00:00.000Z/2014-05-02T00:00:00.000Z" ],
              "dataSource" : {
                "name" : "color_size_shape_shapes",
                "type" : "table"
              },
              "aggregations": [
                    { "name": "width", "fieldName": "width", "type": "longSum" }
              ],
              "postAggregations": [],
              "context": {}
            }"""
    }

    @Override
    String getFakeDruidResponse() {
        """[
              {
                "version" : "v1",
                "timestamp" : "2014-05-01T00:00:00.000Z",
                "event" : {
                  "color" : "color1",
                  "shape" : "shape1",
                  "size" : "size1",
                  "width" : 111
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-05-01T00:00:00.000Z",
                "event" : {
                  "color" : "color2",
                  "shape" : "shape2",
                  "size" : "size2",
                  "width" : 222
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-05-01T00:00:00.000Z",
                "event" : {
                  "color" : "color3",
                  "shape" : "shape3",
                  "size" : "size3",
                  "width" : 333
                }
              }
            ]"""
    }
}
