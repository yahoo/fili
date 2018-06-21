// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.web.endpoints.BaseDataServletComponentSpec
import com.yahoo.bard.webservice.web.endpoints.DataServlet
import com.yahoo.wiki.webservice.application.WikiJerseyTestBinder
import com.yahoo.wiki.webservice.data.config.names.WikiDruidTableName
import com.yahoo.wiki.webservice.data.config.names.WikiLogicalTableName

class SingleDimensionMultipleComplexMetricDataServletSpec extends BaseDataServletComponentSpec {

    @Override
    def setup() {
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        dimensionStore.findByApiName("page").with {
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
        return "data/${WikiLogicalTableName.WIKIPEDIA.asName()}/hour/${"page"}"
    }

    @Override
    JerseyTestBinder buildTestBinder() {
        new WikiJerseyTestBinder(getResourceClasses())
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
            "metrics" : [
                    "${"count"}",
                    "${"added"}",
                    "${"delta"}",
            ],
            "dateTime": [
                "2014-06-02%2F2014-06-09"
            ],
            "asyncAfter": ["never"]
        ]
    }

    @Override
    String getExpectedApiResponse() {
        """{
              "rows" : [
                  {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "${"page"}|id" : "Foo",
                    "${"page"}|desc" : "FooDesc",
                    "${"count"}" : 10,
                    "${"added"}" : 10,
                    "${"delta"}" : 20
                  },
                  {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "${"page"}|id" : "Bar",
                    "${"page"}|desc" : "BarDesc",
                    "${"count"}" : 11,
                    "${"added"}" : 11,
                    "${"delta"}" : 22
                  },
                  {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "${"page"}|id" : "Baz",
                    "${"page"}|desc" : "BazDesc",
                    "${"count"}" : 12,
                    "${"added"}" : 12,
                    "${"delta"}" : 24
                  }
                ]
            }"""
    }

    @Override
    String getExpectedDruidQuery() {
    """{
          "aggregations": [
            {
              "name": "${"count"}",
              "type": "count"
            },
            {
              "fieldName": "${"added"}",
              "name": "${"added"}",
              "type": "doubleSum"
            },
            {
              "fieldName": "${"delta"}",
              "name": "${"delta"}",
              "type": "doubleSum"
            }
          ],
          "context": {},
          "dataSource": {
            "name": "${WikiDruidTableName.WIKITICKER.asName()}",
            "type": "table"
          },
          "dimensions": [
            "page"
          ],
          "granularity": {
            "period": "PT1H",
            "timeZone":"UTC",
            "type": "period"
          },
          "intervals": [
            "2014-06-02T00:00:00.000Z/2014-06-09T00:00:00.000Z"
          ],
          "postAggregations": [],
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
                  "${"page"}" : "Foo",
                  "${"count"}" : 10,
                  "${"added"}" : 10,
                  "${"delta"}" : 20
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                  "${"page"}" : "Bar",
                  "${"count"}" : 11,
                  "${"added"}" : 11,
                  "${"delta"}" : 22
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                  "${"page"}" : "Baz",
                  "${"count"}" : 12,
                  "${"added"}" : 12,
                  "${"delta"}" : 24
                }
              }
        ]"""
    }
}
