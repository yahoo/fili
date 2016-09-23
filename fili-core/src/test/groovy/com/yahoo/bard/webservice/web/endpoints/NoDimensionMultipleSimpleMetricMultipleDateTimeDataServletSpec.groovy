// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.config.FeatureFlag

import org.joda.time.Interval

import spock.lang.Ignore
/**
 * Partial data feature does not support multiple request intervals.
 * This test is ignored until a solid use case for multiple request intervals.
 */
@Ignore
class NoDimensionMultipleSimpleMetricMultipleDateTimeDataServletSpec extends BaseDataServletComponentSpec {

    def setup() {
        Interval interval = new Interval("2010-01-01/2500-12-31")
        populatePhysicalTableCacheIntervals(interval)
    }

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        return "data/network/week/"
    }

    @Override
    String getExpectedDruidQuery() {
        """{
              "queryType" : "groupBy",
              "dataSource" : {
                  "name" : "network_slice",
                  "type" : "table"
              },
              "dimensions" : [],
              "aggregations" : [ {
                  "name" : "pageViews",
                  "fieldName" : "other_page_views",
                  "type" : "longSum"
              }, {
                  "name" : "timespent_secs",
                  "fieldName" : "timespent",
                  "type" : "longSum"
              } ],
              "postAggregations": [ {
                  "type": "arithmetic",
                  "name": "timeSpent",
                  "fn": "/",
                  "fields": [ {
                      "type": "fieldAccess",
                      "fieldName": "timespent_secs"
                  }, {
                      "name":"seconds_per_minute",
                      "type": "constant",
                      "value": 60
                  } ]
              } ],
              "intervals": [
                    "2014-01-01T00:00:00.000Z/2014-01-07T00:00:00.000Z",
                    "2014-01-08T00:00:00.000Z/2014-01-14T00:00:00.000Z",
                    "2014-01-15T00:00:00.000Z/2014-01-21T00:00:00.000Z",
                    "2014-01-22T00:00:00.000Z/2014-01-28T00:00:00.000Z",
                    "2014-01-29T00:00:00.000Z/2014-02-04T00:00:00.000Z",
                    "2014-02-05T00:00:00.000Z/2014-02-11T00:00:00.000Z",
                    "2014-02-12T00:00:00.000Z/2014-02-18T00:00:00.000Z",
                    "2014-02-19T00:00:00.000Z/2014-02-25T00:00:00.000Z",
                    "2014-02-26T00:00:00.000Z/2014-03-04T00:00:00.000Z",
                    "2014-03-05T00:00:00.000Z/2014-03-11T00:00:00.000Z",
                    "2014-03-12T00:00:00.000Z/2014-03-18T00:00:00.000Z",
                    "2014-03-19T00:00:00.000Z/2014-03-25T00:00:00.000Z",
                    "2014-03-26T00:00:00.000Z/2014-04-01T00:00:00.000Z",
                    "2014-04-02T00:00:00.000Z/2014-04-08T00:00:00.000Z",
                    "2014-04-09T00:00:00.000Z/2014-04-15T00:00:00.000Z",
                    "2014-04-16T00:00:00.000Z/2014-04-22T00:00:00.000Z",
                    "2014-04-23T00:00:00.000Z/2014-04-29T00:00:00.000Z",
                    "2014-04-30T00:00:00.000Z/2014-05-06T00:00:00.000Z"
              ],
              "granularity" : {
                  "type" : "period",
                  "period" : "P1W"
              },
              "context": {}
            }"""
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["pageViews", "timeSpent"],
                "dateTime": [
                        "2014-01-01%2F2014-01-07",
                        "2014-01-08%2F2014-01-14",
                        "2014-01-15%2F2014-01-21",
                        "2014-01-22%2F2014-01-28",
                        "2014-01-29%2F2014-02-04",
                        "2014-02-05%2F2014-02-11",
                        "2014-02-12%2F2014-02-18",
                        "2014-02-19%2F2014-02-25",
                        "2014-02-26%2F2014-03-04",
                        "2014-03-05%2F2014-03-11",
                        "2014-03-12%2F2014-03-18",
                        "2014-03-19%2F2014-03-25",
                        "2014-03-26%2F2014-04-01",
                        "2014-04-02%2F2014-04-08",
                        "2014-04-09%2F2014-04-15",
                        "2014-04-16%2F2014-04-22",
                        "2014-04-23%2F2014-04-29",
                        "2014-04-30%2F2014-05-06"
                ]
        ]
    }

    @Override
    String getFakeDruidResponse() {
        """[
              {
                "version" : "v1",
                "timestamp" : "2014-01-01T00:00:00.000Z",
                "event" : {
                  "pageViews" : 10,
                  "timeSpent" : 11
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-01-08T00:00:00.000Z",
                "event" : {
                  "pageViews" : 12,
                  "timeSpent" : 13
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-01-15T00:00:00.000Z",
                "event" : {
                  "pageViews" : 14,
                  "timeSpent" : 15
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-01-22T00:00:00.000Z",
                "event" : {
                  "pageViews" : 16,
                  "timeSpent" : 17
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-01-29T00:00:00.000Z",
                "event" : {
                  "pageViews" : 18,
                  "timeSpent" : 19
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-02-05T00:00:00.000Z",
                "event" : {
                  "pageViews" : 20,
                  "timeSpent" : 21
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-02-12T00:00:00.000Z",
                "event" : {
                  "pageViews" : 22,
                  "timeSpent" : 23
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-02-19T00:00:00.000Z",
                "event" : {
                  "pageViews" : 24,
                  "timeSpent" : 25
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-02-26T00:00:00.000Z",
                "event" : {
                  "pageViews" : 26,
                  "timeSpent" : 27
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-03-05T00:00:00.000Z",
                "event" : {
                  "pageViews" : 28,
                  "timeSpent" : 29
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-03-12T00:00:00.000Z",
                "event" : {
                  "pageViews" : 30,
                  "timeSpent" : 31
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-03-19T00:00:00.000Z",
                "event" : {
                  "pageViews" : 32,
                  "timeSpent" : 33
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-03-26T00:00:00.000Z",
                "event" : {
                  "pageViews" : 34,
                  "timeSpent" : 35
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-04-02T00:00:00.000Z",
                "event" : {
                  "pageViews" : 36,
                  "timeSpent" : 37
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-04-09T00:00:00.000Z",
                "event" : {
                  "pageViews" : 38,
                  "timeSpent" : 39
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-04-16T00:00:00.000Z",
                "event" : {
                  "pageViews" : 40,
                  "timeSpent" : 41
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-04-23T00:00:00.000Z",
                "event" : {
                  "pageViews" : 42,
                  "timeSpent" : 43
                }
              } , {
                "version" : "v1",
                "timestamp" : "2014-04-30T00:00:00.000Z",
                "event" : {
                  "pageViews" : 44,
                  "timeSpent" : 45
                }
              }
            ]"""
    }

    @Override
    String getExpectedApiResponse() {
        String metadata = "";
        if (FeatureFlag.PARTIAL_DATA) {
            metadata = """,
                        "meta" : {
                                   "missingIntervals" : []
                                  }
                        """
        }

        """{
            "rows": [
                {
                  "dateTime": "2014-01-01 00:00:00.000",
                  "pageViews": 10,
                  "timeSpent": 11
                },
                {
                  "dateTime": "2014-01-08 00:00:00.000",
                  "pageViews": 12,
                  "timeSpent": 13
                },
                {
                  "dateTime": "2014-01-15 00:00:00.000",
                  "pageViews": 14,
                  "timeSpent": 15
                },
                {
                  "dateTime": "2014-01-22 00:00:00.000",
                  "pageViews": 16,
                  "timeSpent": 17
                },
                {
                  "dateTime": "2014-01-29 00:00:00.000",
                  "pageViews": 18,
                  "timeSpent": 19
                },
                {
                  "dateTime": "2014-02-05 00:00:00.000",
                  "pageViews": 20,
                  "timeSpent": 21
                },
                {
                  "dateTime": "2014-02-12 00:00:00.000",
                  "pageViews": 22,
                  "timeSpent": 23
                },
                {
                  "dateTime": "2014-02-19 00:00:00.000",
                  "pageViews": 24,
                  "timeSpent": 25
                },
                {
                  "dateTime": "2014-02-26 00:00:00.000",
                  "pageViews": 26,
                  "timeSpent": 27
                },
                {
                  "dateTime": "2014-03-05 00:00:00.000",
                  "pageViews": 28,
                  "timeSpent": 29
                },
                {
                  "dateTime": "2014-03-12 00:00:00.000",
                  "pageViews": 30,
                  "timeSpent": 31
                },
                {
                  "dateTime": "2014-03-19 00:00:00.000",
                  "pageViews": 32,
                  "timeSpent": 33
                },
                {
                  "dateTime": "2014-03-26 00:00:00.000",
                  "pageViews": 34,
                  "timeSpent": 35
                },
                {
                  "dateTime": "2014-04-02 00:00:00.000",
                  "pageViews": 36,
                  "timeSpent": 37
                },
                {
                  "dateTime": "2014-04-09 00:00:00.000",
                  "pageViews": 38,
                  "timeSpent": 39
                },
                {
                  "dateTime": "2014-04-16 00:00:00.000",
                  "pageViews": 40,
                  "timeSpent": 41
                },
                {
                  "dateTime": "2014-04-23 00:00:00.000",
                  "pageViews": 42,
                  "timeSpent": 43
                },
                {
                  "dateTime": "2014-04-30 00:00:00.000",
                  "pageViews": 44,
                  "timeSpent": 45
                }
            ]
            $metadata
        }"""
    }
}
