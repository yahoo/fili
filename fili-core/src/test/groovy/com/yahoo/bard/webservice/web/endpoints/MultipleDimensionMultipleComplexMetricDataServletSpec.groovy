// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
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
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "all_mail", "All Mail"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "all_search", "All Search"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "all_front_page", "All Front Page"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "all_tumblr", "All Tumblr"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "all_registration", "All Registration"))
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
                "color|desc-in[All Mail,All Search,All Front Page]",
                "shape|desc-in[App]",
                "color|desc-notin[All Tumblr,All Registration]"
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
                      "value": "all_registration"
                    },
                    {
                      "dimension": "color",
                      "type": "selector",
                      "value": "all_tumblr"
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
                    "value": "all_front_page"
                  },
                  {
                    "dimension": "color",
                    "type": "selector",
                    "value": "all_mail"
                  },
                  {
                    "dimension": "color",
                    "type": "selector",
                    "value": "all_search"
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
      "name": "dayAvgBcookies",
      "type": "arithmetic"
    },    {
      "fields": [
        {
          "fieldName": "bcookies_sum",
          "type": "fieldAccess"
        },
        {
          "fieldName": "count",
          "type": "fieldAccess"
        }
      ],
      "fn": "/",
      "name": "dayAvgBcookies",
      "type": "arithmetic"
    },
    {
      "field": {
        "fields": [
          {
            "fieldName": "bcookiesNoYuid",
            "type": "fieldAccess"
          },
          {
            "fieldName": "regBcookies",
            "type": "fieldAccess"
          }
        ],
        "func": "UNION",
        "name": "bcookies",
        "type": "sketchSetOper"
      },
      "name": "bcookies",
      "type": "sketchEstimate"
    },
    {
      "fields": [
        {
          "fieldName": "bcookies_sum",
          "type": "fieldAccess"
        },
        {
          "fieldName": "count",
          "type": "fieldAccess"
        }
      ],
      "fn": "/",
      "name": "dayAvgBcookies",
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
          "fieldName": "regBcookies_estimate_sum",
          "type": "fieldAccess"
        }
      ],
      "fn": "/",
      "name": "dayAvgRegBcookies",
      "type": "arithmetic"
    },
    {
      "fields": [
        {
          "fieldName": "count",
          "type": "fieldAccess"
        },
        {
          "fieldName": "regUsers_sum",
          "type": "fieldAccess"
        }
      ],
      "fn": "/",
      "name": "dayAvgRegUsers",
      "type": "arithmetic"
    },
    {
      "fields": [
        {
          "fieldName": "count",
          "type": "fieldAccess"
        },
        {
          "fieldName": "unregBcookies_sum",
          "type": "fieldAccess"
        }
      ],
      "fn": "/",
      "name": "dayAvgUnregBcookies",
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
