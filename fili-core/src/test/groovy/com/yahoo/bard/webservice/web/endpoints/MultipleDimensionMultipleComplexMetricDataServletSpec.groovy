// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary

import spock.lang.Ignore

@Ignore("Need more complex test metrics to do what test needs to test.")
class MultipleDimensionMultipleComplexMetricDataServletSpec extends BaseDataServletComponentSpec {

    @Override
    def setup() {
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        dimensionStore.findByApiName("color").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "all_foo", "All foo"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "all_bar", "All bar"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "all_baz", "All baz"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "all_waz", "All waz"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "all_biz", "All biz"))
        }
        dimensionStore.findByApiName("shape").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "App", "App"))
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
            "metrics" : [
                "height",
                "width",
                "depth",
                "area",
                "volume",
                "users",
                "otherUsers",
                "dayAvgUsers",
                "dayAvgOtherUsers"
            ],
            "dateTime": [
                "2014-06-09 00:00:00.000/2014-06-16 00:00:00.000"
            ],
            "filters" : [
                "color|desc-in[All foo,All bar,All baz]",
                "shape|desc-in[App]",
                "color|desc-notin[All waz,All biz]"
            ]
        ]
    }

    @Override
    String getExpectedDruidQuery() {
        """{
  "aggregations": [
    {
      "fieldName": "users_estimate",
      "name": "users_estimate_sum",
      "type": "longSum"
    },
    {
      "fieldName": "other_users_estimate",
      "name": "other_users_estimate_sum",
      "type": "longSum"
    },
    {
      "fieldName": "one",
      "name": "count",
      "type": "longSum"
    },
    {
      "fieldName": "height",
      "name": "height",
      "type": "longSum"
    },
    {
      "fieldName": "width",
      "name": "width",
      "type": "longSum"
    },
    {
      "fieldName": "depth",
      "name": "depth",
      "type": "longSum"
    },
    {
      "fieldName": "users",
      "name": "users",
      "size": 16384,
      "type": "sketchCount"
    },
    {
      "fieldName": "otherUsers",
      "name": "otherUsers",
      "size": 16384,
      "type": "sketchCount"
    }
  ],
  "dataSource": {
    "query": {
      "aggregations": [
        {
          "fieldName": "height",
          "name": "height",
          "type": "longSum"
        },
        {
          "fieldName": "width",
          "name": "width",
          "type": "longSum"
        },
        {
          "fieldName": "users",
          "name": "otherUsers",
          "size": 16384,
          "type": "sketchCount"
        },
        {
          "fieldName": "depth",
          "name": "depth",
          "type": "longSum"
        },
        {
          "fieldName": "users",
          "name": "users",
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
      "filter": {
        "fields": [
          {
            "fields": [
              {
                "dimension": "shapes",
                "type": "selector",
                "value": "App"
              }
            ],
            "type": "or"
          },
          {
            "fields": [
              {
                "field": {
                  "fields": [
                    {
                      "dimension": "color",
                      "type": "selector",
                      "value": "all_biz"
                    },
                    {
                      "dimension": "color",
                      "type": "selector",
                      "value": "all_waz"
                    }
                  ],
                  "type": "or"
                },
                "type": "not"
              },
              {
                "fields": [
                  {
                    "dimension": "color",
                    "type": "selector",
                    "value": "all_baz"
                  },
                  {
                    "dimension": "color",
                    "type": "selector",
                    "value": "all_foo"
                  },
                  {
                    "dimension": "color",
                    "type": "selector",
                    "value": "all_bar"
                  }
                ],
                "type": "or"
              }
            ],
            "type": "and"
          }
        ],
        "type": "and"
      },
      "granularity": {
        "period": "P1D",
        "type": "period"
      },
      "intervals": [
        "2014-06-09T00:00:00.000Z/2014-06-16T00:00:00.000Z"
      ],
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
          "field": {
            "fieldName": "other_users",
            "type": "fieldAccess"
          },
          "name": "other_users_estimate",
          "type": "sketchEstimate"
        },
        {
          "name": "one",
          "type": "constant",
          "value": 1
        }
      ],
      "queryType": "groupBy"
    },
    "type": "query"
  },
  "dimensions": [
    "color"
  ],
  "granularity": {
    "period": "P1W",
    "type": "period"
  },
  "intervals": [
    "2014-06-09T00:00:00.000Z/2014-06-16T00:00:00.000Z"
  ],
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
      "name": "dayAvgFoos",
      "type": "arithmetic"
    },    {
      "fields": [
        {
          "fieldName": "foos_sum",
          "type": "fieldAccess"
        },
        {
          "fieldName": "count",
          "type": "fieldAccess"
        }
      ],
      "fn": "/",
      "name": "dayAvgFoos",
      "type": "arithmetic"
    },
    {
      "field": {
        "fields": [
          {
            "fieldName": "foosNoBar",
            "type": "fieldAccess"
          },
          {
            "fieldName": "regFoos",
            "type": "fieldAccess"
          }
        ],
        "func": "UNION",
        "name": "foos",
        "type": "sketchSetOper"
      },
      "name": "foos",
      "type": "sketchEstimate"
    },
    {
      "fields": [
        {
          "fieldName": "foos_sum",
          "type": "fieldAccess"
        },
        {
          "fieldName": "count",
          "type": "fieldAccess"
        }
      ],
      "fn": "/",
      "name": "dayAvgFoos",
      "type": "arithmetic"
    },
    {
      "fields": [
        {
          "fieldName": "count",
          "type": "fieldAccess"
        },
        {
          "fieldName": "pageViews",
          "type": "fieldAccess"
        }
      ],
      "fn": "/",
      "name": "dayAvgPageViews",
      "type": "arithmetic"
    },
    {
      "fields": [
        {
          "fieldName": "count",
          "type": "fieldAccess"
        },
        {
          "fieldName": "regFoos_estimate_sum",
          "type": "fieldAccess"
        }
      ],
      "fn": "/",
      "name": "dayAvgRegFoos",
      "type": "arithmetic"
    },
    {
      "fields": [
        {
          "fieldName": "count",
          "type": "fieldAccess"
        },
        {
          "fieldName": "waz_sum",
          "type": "fieldAccess"
        }
      ],
      "fn": "/",
      "name": "dayAvgWaz",
      "type": "arithmetic"
    },
    {
      "fields": [
        {
          "fieldName": "count",
          "type": "fieldAccess"
        },
        {
          "fieldName": "unregFoos_sum",
          "type": "fieldAccess"
        }
      ],
      "fn": "/",
      "name": "dayAvgUnregFoos",
      "type": "arithmetic"
    }
  ],
  "queryType": "groupBy"
}"""
    }

    @Override
    public String getExpectedApiResponse() {
        """{ "rows": [ ] }"""
    }
}
