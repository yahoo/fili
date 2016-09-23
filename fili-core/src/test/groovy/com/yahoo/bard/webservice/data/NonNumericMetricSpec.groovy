// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_STRING_METRIC
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_BOOLEAN_METRIC
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_JSON_NODE_METRIC
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_NULL_METRIC

import com.yahoo.bard.webservice.web.endpoints.BaseDataServletComponentSpec
import com.yahoo.bard.webservice.web.endpoints.DataServlet

/**
 * Performs an end to end feature test of non-numeric metrics (booleans, Strings, and JsonNodes). The (rather
 * stubby) LogicalMetrics that define the complex metrics, and the sample mappers used to process the complex metrics
 * can be found in {@link com.yahoo.bard.webservice.data.config.metric.NonNumericMetrics}.
 */
class NonNumericMetricSpec extends BaseDataServletComponentSpec {

    @Override
    Class<?>[] getResourceClasses() {
        return [DataServlet.class]
    }

    @Override
    String getTarget() {
        "data/shapes/day"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                metrics: [A_STRING_METRIC, A_NULL_METRIC, A_BOOLEAN_METRIC, A_JSON_NODE_METRIC].collect {it.asName()},
                dateTime: ["2014-09-01/2014-09-02"]
        ]
    }

    /*
     * This is purely a placeholder to get through the query building stages that are not related
     * to handling non-numeric metrics. With the exception of the names of the columns, it has no relation to the fake
     * Druid response.
     */
    @Override
    String getExpectedDruidQuery() {
        """{
                "queryType": "timeseries",
                "intervals": ["2014-09-01T00:00:00.000Z/2014-09-02T00:00:00.000Z"],
                    "granularity": ${getTimeGrainString()},
                "dataSource": {
                    "name": "color_shapes",
                    "type": "table"
                },
                "aggregations": [
                    {
                        "fieldName": "height",
                        "name":"${A_STRING_METRIC.asName()}",
                        "type": "min"
                    },
                    {
                        "fieldName": "height",
                        "name":"${A_NULL_METRIC.asName()}",
                        "type": "min"
                    },
                    {
                        "fieldName": "height",
                        "name":"${A_JSON_NODE_METRIC.asName()}",
                        "type": "min"
                    },
                    {
                        "fieldName": "height",
                        "name":"${A_BOOLEAN_METRIC.asName()}",
                        "type": "min"
                    }
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
                  "timestamp" : "2014-09-01T00:00:00.000Z",
                  "result" : {
                      "color" : "Crimson",
                      "${A_STRING_METRIC.asName()}" : "Crimson is Red. Or is it?",
                      "${A_BOOLEAN_METRIC.asName()}" : true,
                      "${A_JSON_NODE_METRIC.asName()}" : {
                          "clarification": "The stringMetric is totally lying.",
                          "second_thoughts": "Or is it?"
                      }
                  }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-09-01T00:00:00.000Z",
                "result" : {
                      "color" : "Red",
                      "${A_STRING_METRIC.asName()}" : "Red is Crimson. Or is it?",
                      "${A_BOOLEAN_METRIC.asName()}" : false,
                      "${A_JSON_NODE_METRIC.asName()}" : {
                          "clarification": "The stringMetric is totally telling the truth.",
                          "second_thoughts": "Or is it?"
                      }
                }
              }
            ]
        """
    }

    @Override
    String getExpectedApiResponse() {
        String clarification = "The stringMetric is totally lying."
        """{
              "rows" : [
                  {
                        "dateTime" : "2014-09-01 00:00:00.000",
                        "${A_STRING_METRIC.asName()}" : "Crimson is Red. Or is it?Crimson is Red. Or is it?",
                        "${A_BOOLEAN_METRIC.asName()}" : true,
                        "${A_NULL_METRIC.asName()}" : null,
                        "${A_JSON_NODE_METRIC.asName()}" : {
                            "clarification": "$clarification",
                            "second_thoughts": "Or is it?",
                            "length": ${clarification.length()}
                        }
                  }
                ]
        }"""
    }
}
