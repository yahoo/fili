// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.logging.TestLogAppender
import com.yahoo.bard.webservice.web.LoggingTestUtils
import com.yahoo.bard.webservice.web.filters.BardLoggingFilter

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Shared
import spock.lang.Timeout
/**
 * Checks that all of the table/grain pairs have the dimensions and columns expected.
 */
@spock.lang.Ignore
@Timeout(30)    // Fail test if hangs
class ExpectedTablesFullViewEndpointSpec extends BaseTableServletComponentSpec {

    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    @Shared
    TestLogAppender logAppender

    def setupSpec() {
        // Hook with test appender
        logAppender = new TestLogAppender()
    }

    def cleanup() {
        logAppender.clear()
    }

    def cleanupSpec() {
        logAppender.close()
    }

    @Override
    Class<?>[] getResourceClasses() {
        //We had an issue where the results of this query were successfully returned to the user, but when we attempted
        //to log the request, a NullPointerException was thrown and logged instead, because the TablesApiRequest hadn't
        //paginated properly. By adding the `BardLoggingFilter`, we can inspect the log output of the test and get
        //a log output very similar to that seen in production, in case the problem rears its ugly head again.
        [TablesServlet.class, BardLoggingFilter.class]
    }

    @Override
    String getTarget() {
        return "tables"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        return ["format": ["fullview"]]
    }

    @Override
    String getExpectedApiResponse() {
        """{
          "tables": [
            {
              "description": "hourly",
              "longName": "hourly",
              "name": "hourly",
              "category": "General",
              "timeGrains": [
                {
                  "description": "The hourly all grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",
                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "All",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    }
                  ],
                  "name": "all",
                  "retention": ""
                },
                {
                  "description": "The hourly day grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",
                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "Day",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    }
                  ],
                  "name": "day",
                  "retention": ""
                },
                {
                  "description": "The hourly hour grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",
                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "Hour",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    }
                  ],
                  "name": "hour",
                  "retention": ""
                },
                {
                  "description": "The hourly month grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",
                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "Month",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    }
                  ],
                  "name": "month",
                  "retention": ""
                },
                {
                  "description": "The hourly week grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",
                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "Week",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    }
                  ],
                  "name": "week",
                  "retention": ""
                }
              ]
            },
            {
              "description": "hourly_monthly",
              "longName": "hourly_monthly",
              "name": "hourly_monthly",
              "category": "General",
              "timeGrains": [
                {
                  "description": "The hourly_monthly all grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",
                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "All",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    }
                  ],
                  "name": "all",
                  "retention": ""
                },
                {
                  "description": "The hourly_monthly day grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",
                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "Day",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    }
                  ],
                  "name": "day",
                  "retention": ""
                },
                {
                  "description": "The hourly_monthly hour grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",
                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "Hour",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    }
                  ],
                  "name": "hour",
                  "retention": ""
                },
                {
                  "description": "The hourly_monthly month grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",
                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "Month",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    }
                  ],
                  "name": "month",
                  "retention": ""
                },
                {
                  "description": "The hourly_monthly week grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "Week",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    }
                  ],
                  "name": "week",
                  "retention": ""
                }
              ]
            },
            {
              "description": "monthly",
              "longName": "monthly",
              "name": "monthly",
              "category": "General",
              "timeGrains": [
                {
                  "description": "The monthly all grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "All",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    }
                  ],
                  "name": "all",
                  "retention": ""
                },
                {
                  "description": "The monthly day grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "Day",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    }
                  ],
                  "name": "day",
                  "retention": ""
                },
                {
                  "description": "The monthly month grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "Month",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    }
                  ],
                  "name": "month",
                  "retention": ""
                },
                {
                  "description": "The monthly week grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "Week",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    }
                  ],
                  "name": "week",
                  "retention": ""
                }
              ]
            },
            {
              "description": "pets",
              "longName": "pets",
              "name": "pets",
              "category": "General",
              "timeGrains": [
                {
                  "description": "The pets all grain",
                  "dimensions": [
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "breed",
                      "name": "breed",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/breed"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "sex",
                      "name": "sex",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/sex"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "species",
                      "name": "species",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/species"
                    }
                  ],
                  "longName": "All",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/rowNum"
                    }
                  ],
                  "name": "all",
                  "retention": ""
                },
                {
                  "description": "The pets day grain",
                  "dimensions": [
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "breed",
                      "name": "breed",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/breed"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "sex",
                      "name": "sex",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/sex"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "species",
                      "name": "species",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/species"
                    }
                  ],
                  "longName": "Day",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/rowNum"
                    }
                  ],
                  "name": "day",
                  "retention": ""
                },
                {
                  "description": "The pets month grain",
                  "dimensions": [
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "breed",
                      "name": "breed",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/breed"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "sex",
                      "name": "sex",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/sex"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "species",
                      "name": "species",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/species"
                    }
                  ],
                  "longName": "Month",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/rowNum"
                    }
                  ],
                  "name": "month",
                  "retention": ""
                },
                {
                  "description": "The pets week grain",
                  "dimensions": [
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "breed",
                      "name": "breed",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/breed"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "sex",
                      "name": "sex",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/sex"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "species",
                      "name": "species",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/species"
                    }
                  ],
                  "longName": "Week",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/limbs"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/rowNum"
                    }
                  ],
                  "name": "week",
                  "retention": ""
                }
              ]
            },
            {
              "description": "shapes",
              "longName": "shapes",
              "name": "shapes",
              "category": "General",
              "timeGrains": [
                {
                  "description": "The shapes all grain",
                  "dimensions": [
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "color",
                      "name": "color",
                      "storageStrategy":"none",

                      "fields": [
                        {
                          "description":"Blue pigment",
                          "name":"bluePigment"
                        }, {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }, {
                          "description":"Green pigment",
                          "name":"greenPigment"
                        }, {
                          "description":"Red pigment",
                          "name":"redPigment"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/color"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "model",
                      "name": "model",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/model"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "shape",
                      "name": "shape",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/shape"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "size",
                      "name": "size",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/size"
                    },
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "All",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "area",
                      "name": "area",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/area"
                    },
                    {
                      "category": "General",
                      "longName": "booleanMetric",
                      "name": "booleanMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/booleanMetric"
                    },
                    {
                      "category": "General",
                      "longName": "dayAvgOtherUsers",
                      "name": "dayAvgOtherUsers",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgOtherUsers"
                    },
                    {
                      "category": "General",
                      "longName": "dayAvgUsers",
                      "name": "dayAvgUsers",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgUsers"
                    },
                    {
                      "category": "General",
                      "longName": "depth",
                      "name": "depth",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/depth"
                    },
                    {
                      "category": "General",
                      "longName": "height",
                      "name": "height",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/height"
                    },
                    {
                      "category": "General",
                      "longName": "jsonNodeMetric",
                      "name": "jsonNodeMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/jsonNodeMetric"
                    },
                    {
                      "category": "General",
                      "longName": "nullMetric",
                      "name": "nullMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/nullMetric"
                    },
                    {
                      "category": "General",
                      "longName": "otherUsers",
                      "name": "otherUsers",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/otherUsers"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/rowNum"
                    },
                    {
                      "category": "General",
                      "longName": "stringMetric",
                      "name": "stringMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/stringMetric"
                    },
                    {
                      "category": "General",
                      "longName": "users",
                      "name": "users",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/users"
                    },
                    {
                      "category": "General",
                      "longName": "volume",
                      "name": "volume",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/volume"
                    },
                    {
                      "category": "General",
                      "longName": "width",
                      "name": "width",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/width"
                    },
                    {
                      "category": "General",
                      "longName": "scopedWidth",
                      "name": "scopedWidth",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/scopedWidth"
                    }
                  ],
                  "name": "all",
                  "retention": ""
                },
                {
                  "description": "The shapes day grain",
                  "dimensions": [
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "color",
                      "name": "color",
                      "storageStrategy":"none",

                      "fields": [
                        {
                          "description":"Blue pigment",
                          "name":"bluePigment"
                        }, {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }, {
                          "description":"Green pigment",
                          "name":"greenPigment"
                        }, {
                          "description":"Red pigment",
                          "name":"redPigment"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/color"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "model",
                      "name": "model",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/model"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "shape",
                      "name": "shape",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/shape"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "size",
                      "name": "size",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/size"
                    },
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "Day",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "area",
                      "name": "area",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/area"
                    },
                    {
                      "category": "General",
                      "longName": "booleanMetric",
                      "name": "booleanMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/booleanMetric"
                    },
                    {
                      "category": "General",
                      "longName": "depth",
                      "name": "depth",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/depth"
                    },
                    {
                      "category": "General",
                      "longName": "height",
                      "name": "height",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/height"
                    },
                    {
                      "category": "General",
                      "longName": "jsonNodeMetric",
                      "name": "jsonNodeMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/jsonNodeMetric"
                    },
                    {
                      "category": "General",
                      "longName": "nullMetric",
                      "name": "nullMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/nullMetric"
                    },
                    {
                      "category": "General",
                      "longName": "otherUsers",
                      "name": "otherUsers",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/otherUsers"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/rowNum"
                    },
                    {
                      "category": "General",
                      "longName": "stringMetric",
                      "name": "stringMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/stringMetric"
                    },
                    {
                      "category": "General",
                      "longName": "users",
                      "name": "users",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/users"
                    },
                    {
                      "category": "General",
                      "longName": "volume",
                      "name": "volume",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/volume"
                    },
                    {
                      "category": "General",
                      "longName": "width",
                      "name": "width",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/width"
                    },
                    {
                      "category": "General",
                      "longName": "scopedWidth",
                      "name": "scopedWidth",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/scopedWidth"
                    }
                  ],
                  "name": "day",
                  "retention": ""
                },
                {
                  "description": "The shapes month grain",
                  "dimensions": [
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "color",
                      "name": "color",
                      "storageStrategy":"none",

                      "fields": [
                        {
                          "description":"Blue pigment",
                          "name":"bluePigment"
                        }, {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }, {
                          "description":"Green pigment",
                          "name":"greenPigment"
                        }, {
                          "description":"Red pigment",
                          "name":"redPigment"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/color"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "model",
                      "name": "model",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/model"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "shape",
                      "name": "shape",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/shape"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "size",
                      "name": "size",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/size"
                    },
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "Month",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "area",
                      "name": "area",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/area"
                    },
                    {
                      "category": "General",
                      "longName": "booleanMetric",
                      "name": "booleanMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/booleanMetric"
                    },
                    {
                      "category": "General",
                      "longName": "dayAvgOtherUsers",
                      "name": "dayAvgOtherUsers",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgOtherUsers"
                    },
                    {
                      "category": "General",
                      "longName": "dayAvgUsers",
                      "name": "dayAvgUsers",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgUsers"
                    },
                    {
                      "category": "General",
                      "longName": "depth",
                      "name": "depth",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/depth"
                    },
                    {
                      "category": "General",
                      "longName": "height",
                      "name": "height",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/height"
                    },
                    {
                      "category": "General",
                      "longName": "jsonNodeMetric",
                      "name": "jsonNodeMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/jsonNodeMetric"
                    },
                    {
                      "category": "General",
                      "longName": "nullMetric",
                      "name": "nullMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/nullMetric"
                    },
                    {
                      "category": "General",
                      "longName": "otherUsers",
                      "name": "otherUsers",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/otherUsers"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/rowNum"
                    },
                    {
                      "category": "General",
                      "longName": "stringMetric",
                      "name": "stringMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/stringMetric"
                    },
                    {
                      "category": "General",
                      "longName": "users",
                      "name": "users",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/users"
                    },
                    {
                      "category": "General",
                      "longName": "volume",
                      "name": "volume",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/volume"
                    },
                    {
                      "category": "General",
                      "longName": "width",
                      "name": "width",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/width"
                    },
                    {
                      "category": "General",
                      "longName": "scopedWidth",
                      "name": "scopedWidth",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/scopedWidth"
                    }
                  ],
                  "name": "month",
                  "retention": ""
                },
                {
                  "description": "The shapes week grain",
                  "dimensions": [
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "color",
                      "name": "color",
                      "storageStrategy":"none",

                      "fields": [
                        {
                          "description":"Blue pigment",
                          "name":"bluePigment"
                        }, {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }, {
                          "description":"Green pigment",
                          "name":"greenPigment"
                        }, {
                          "description":"Red pigment",
                          "name":"redPigment"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/color"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "model",
                      "name": "model",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/model"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "shape",
                      "name": "shape",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/shape"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "size",
                      "name": "size",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/size"
                    },
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "storageStrategy":"loaded",

                      "fields": [
                        {
                          "description":"Dimension Description",
                          "name":"desc"
                        }, {
                          "description":"Dimension ID",
                          "name":"id"
                        }
                      ],
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/dimensions/other"
                    }
                  ],
                  "longName": "Week",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "area",
                      "name": "area",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/area"
                    },
                    {
                      "category": "General",
                      "longName": "booleanMetric",
                      "name": "booleanMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/booleanMetric"
                    },
                    {
                      "category": "General",
                      "longName": "dayAvgOtherUsers",
                      "name": "dayAvgOtherUsers",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgOtherUsers"
                    },
                    {
                      "category": "General",
                      "longName": "dayAvgUsers",
                      "name": "dayAvgUsers",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/dayAvgUsers"
                    },
                    {
                      "category": "General",
                      "longName": "depth",
                      "name": "depth",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/depth"
                    },
                    {
                      "category": "General",
                      "longName": "height",
                      "name": "height",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/height"
                    },
                    {
                      "category": "General",
                      "longName": "jsonNodeMetric",
                      "name": "jsonNodeMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/jsonNodeMetric"
                    },
                    {
                      "category": "General",
                      "longName": "nullMetric",
                      "name": "nullMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/nullMetric"
                    },
                    {
                      "category": "General",
                      "longName": "otherUsers",
                      "name": "otherUsers",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/otherUsers"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/rowNum"
                    },
                    {
                      "category": "General",
                      "longName": "stringMetric",
                      "name": "stringMetric",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/stringMetric"
                    },
                    {
                      "category": "General",
                      "longName": "users",
                      "name": "users",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/users"
                    },
                    {
                      "category": "General",
                      "longName": "volume",
                      "name": "volume",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/volume"
                    },
                    {
                      "category": "General",
                      "longName": "width",
                      "name": "width",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/width"
                    },
                    {
                      "category": "General",
                      "longName": "scopedWidth",
                      "name": "scopedWidth",
                      "type": "number",
                      "uri": "http://localhost:${jtb.getHarness().getPort()}/metrics/scopedWidth"
                    }
                  ],
                  "name": "week",
                  "retention": ""
                }
              ]
            }
          ]
        }"""
    }

    def "We can successfully log the response to a request of the fullview of the tables"() {
        when: "We make a request for a full view of the tables"
        jtb.getHarness().target("tables").queryParam("format", "fullview").request().get(String.class)

        Map<String, String> loggedResults = LoggingTestUtils.extractResultsFromLogs(logAppender, MAPPER)

        then: "The message is logged correctly"
        loggedResults.status == '"OK"'
        loggedResults.code == '200'
        loggedResults.logMessage == '"Successful request"'

        and: "A MappableException was not logged"
        !logAppender.getMessages().find {it.contains("MappableException")}
    }
}
