// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary

class AlternateTimezoneDailyAvgWeekSpec extends BaseDataServletComponentSpec {

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
                "metrics": ["dayAvgUsers"],
                "dateTime": ["2014-09-01%2F2014-09-08"],
                "timeZone": ["US/Eastern"]
        ]
    }

    @Override
    String getExpectedDruidQuery() {
        """{
            "aggregations": [
                {
                    "fieldName": "one",
                    "name": "count",
                    "type": "longSum"
                },
                {
                    "fieldName": "users_estimate",
                    "name": "users_estimate_sum",
                    "type": "doubleSum"
                }
            ],
            "dataSource": {
                "query": {
                    "aggregations": [
                            {
                                "fieldName": "users",
                                "name": "users",
                                "size": 16384,
                                "type": "sketchMerge"
                            }
                    ],
                    "dataSource": {
        """ +
        // The table color_shapes_hourly is selected here instead of color_shapes because
        // this is testing alternate timezones and the color_shapes table has day boundaries
        // on the UTC day.
        """
                        "name": "color_shapes_hourly",
                        "type": "table"
                    },
                    "dimensions": ["color"],
                    "granularity": ${getTimeGrainString("day", "US/Eastern")},
                    "intervals": ["2014-09-01T00:00:00.000-04:00/2014-09-08T00:00:00.000-04:00"],
                    "postAggregations": [
                            {
                                "field": {
                                "fieldName": "users",
                                "type": "fieldAccess"
                            },
                                "name": "users_estimate",
                                "type": "sketchEstimate"
                            },
                            {
                                "name": "one",
                                "type": "constant",
                                "value": 1.0
                            }
                    ],
                    "queryType": "groupBy",
                    "context": {}
                },
                "type": "query"
            },
            "dimensions": ["color"],
            "granularity": ${getTimeGrainString("week", "US/Eastern")},
            "intervals": ["2014-09-01T00:00:00.000-04:00/2014-09-08T00:00:00.000-04:00"],
            "postAggregations": [
                {
                    "fields": [
                        {
                            "fieldName": "users_estimate_sum",
                            "type": "fieldAccess"
                        },
                        {
                            "fieldName": "count",
                            "type": "fieldAccess"
                        }
                    ],
                    "fn": "/",
                    "name": "dayAvgUsers",
                    "type": "arithmetic"
                }
            ],
            "queryType": "groupBy",
            "context": {}
        }"""
    }

    @Override
    String getFakeDruidResponse() {
        """[
              {
                "version" : "v1",
                "timestamp" : "2014-09-01T00:00:00.000-04:00",
                "event" : {
                  "color" : "Foo",
                  "dayAvgUsers" : 10
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-09-01T00:00:00.000-04:00",
                "event" : {
                  "color" : "Bar",
                  "dayAvgUsers" : 11
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-09-01T00:00:00.000-04:00",
                "event" : {
                  "color" : "Baz",
                  "dayAvgUsers" : 12
                }
              }
            ]
"""
    }

    @Override
    String getExpectedApiResponse() {
        """{
              "rows" : [
                  {
                    "dateTime" : "2014-09-01 00:00:00.000",
                    "color|id" : "Foo",
                    "color|desc" : "FooDesc",
                    "dayAvgUsers" : 10
                  },
                  {
                    "dateTime" : "2014-09-01 00:00:00.000",
                    "color|id" : "Bar",
                    "color|desc" : "BarDesc",
                    "dayAvgUsers" : 11
                  },
                  {
                    "dateTime" : "2014-09-01 00:00:00.000",
                    "color|id" : "Baz",
                    "color|desc" : "BazDesc",
                    "dayAvgUsers" : 12
                  }
                ]
            }"""
    }
}
