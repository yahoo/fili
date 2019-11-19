// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.web.endpoints.BaseDataServletComponentSpec
import com.yahoo.bard.webservice.web.endpoints.DataServlet
import com.yahoo.wiki.webservice.application.WikiJerseyTestBinder
import com.yahoo.wiki.webservice.data.config.names.WikiApiDimensionName
import com.yahoo.wiki.webservice.data.config.names.WikiApiMetricName
import com.yahoo.wiki.webservice.data.config.names.WikiDruidMetricName
import com.yahoo.wiki.webservice.data.config.names.WikiDruidTableName

class SingleDimensionMultipleComplexMetricDataServletSpec extends BaseDataServletComponentSpec {

    @Override
    def setup() {
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        dimensionStore.findByApiName(WikiApiDimensionName.PAGE.asName()).with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Foo"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Bar"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Baz"))
        }
    }

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        return "data/${WikiDruidTableName.WIKITICKER.asName()}/hour/${WikiApiDimensionName.PAGE.asName()}"
    }

    @Override
    JerseyTestBinder buildTestBinder() {
        new WikiJerseyTestBinder(getResourceClasses())
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics"   : [
                        "${WikiApiMetricName.COUNT.asName()}",
                        "${WikiApiMetricName.ADDED.asName()}",
                        "${WikiApiMetricName.DELTA.asName()}",
                ],
                "dateTime"  : [
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
                    "${WikiApiDimensionName.PAGE.asName()}|id" : "Foo",
                    "${WikiApiMetricName.COUNT.asName()}" : 10,
                    "${WikiApiMetricName.ADDED.asName()}" : 10,
                    "${WikiApiMetricName.DELTA.asName()}" : 20
                  },
                  {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "${WikiApiDimensionName.PAGE.asName()}|id" : "Bar",
                    "${WikiApiMetricName.COUNT.asName()}" : 11,
                    "${WikiApiMetricName.ADDED.asName()}" : 11,
                    "${WikiApiMetricName.DELTA.asName()}" : 22
                  },
                  {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "${WikiApiDimensionName.PAGE.asName()}|id" : "Baz",
                    "${WikiApiMetricName.COUNT.asName()}" : 12,
                    "${WikiApiMetricName.ADDED.asName()}" : 12,
                    "${WikiApiMetricName.DELTA.asName()}" : 24
                  }
                ]
            }"""
    }

    @Override
    String getExpectedDruidQuery() {
        """{
          "aggregations": [
            {
              "fieldName": "${WikiDruidMetricName.COUNT.asName()}",
              "name": "${WikiApiMetricName.COUNT.asName()}",
              "type": "doubleSum"
            },
            {
              "fieldName": "${WikiDruidMetricName.ADDED.asName()}",
              "name": "${WikiApiMetricName.ADDED.asName()}",
              "type": "doubleSum"
            },
            {
              "fieldName": "${WikiDruidMetricName.DELTA.asName()}",
              "name": "${WikiApiMetricName.DELTA.asName()}",
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
                  "${WikiApiDimensionName.PAGE.asName()}" : "Foo",
                  "${WikiApiMetricName.COUNT.asName()}" : 10,
                  "${WikiApiMetricName.ADDED.asName()}" : 10,
                  "${WikiApiMetricName.DELTA.asName()}" : 20
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                  "${WikiApiDimensionName.PAGE.asName()}" : "Bar",
                  "${WikiApiMetricName.COUNT.asName()}" : 11,
                  "${WikiApiMetricName.ADDED.asName()}" : 11,
                  "${WikiApiMetricName.DELTA.asName()}" : 22
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                  "${WikiApiDimensionName.PAGE.asName()}" : "Baz",
                  "${WikiApiMetricName.COUNT.asName()}" : 12,
                  "${WikiApiMetricName.ADDED.asName()}" : 12,
                  "${WikiApiMetricName.DELTA.asName()}" : 24
                }
              }
        ]"""
    }
}
