// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.logging.TestLogAppender
import com.yahoo.bard.webservice.web.LoggingTestUtils
import com.yahoo.bard.webservice.web.filters.BardLoggingFilter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import spock.lang.Shared
import spock.lang.Timeout

/**
 * Checks that all of the table/grain pairs have the dimensions and columns expected.
 */
@Timeout(30)    // Fail test if hangs
class ExpectedTablesFullViewEndpointSpec extends BaseTableServletComponentSpec {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

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
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "All",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    }
                  ],
                  "name": "all",
                  "retention": "P1Y"
                },
                {
                  "description": "The hourly day grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "Day",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    }
                  ],
                  "name": "day",
                  "retention": "P1Y"
                },
                {
                  "description": "The hourly hour grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "Hour",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    }
                  ],
                  "name": "hour",
                  "retention": "P1Y"
                },
                {
                  "description": "The hourly month grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "Month",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    }
                  ],
                  "name": "month",
                  "retention": "P1Y"
                },
                {
                  "description": "The hourly week grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "Week",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    }
                  ],
                  "name": "week",
                  "retention": "P1Y"
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
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "All",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "uri": "http://localhost:9998/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    }
                  ],
                  "name": "all",
                  "retention": "P1Y"
                },
                {
                  "description": "The hourly_monthly day grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "Day",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "uri": "http://localhost:9998/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    }
                  ],
                  "name": "day",
                  "retention": "P1Y"
                },
                {
                  "description": "The hourly_monthly hour grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "Hour",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    }
                  ],
                  "name": "hour",
                  "retention": "P1Y"
                },
                {
                  "description": "The hourly_monthly month grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "Month",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "uri": "http://localhost:9998/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    }
                  ],
                  "name": "month",
                  "retention": "P1Y"
                },
                {
                  "description": "The hourly_monthly week grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "Week",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "uri": "http://localhost:9998/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    }
                  ],
                  "name": "week",
                  "retention": "P1Y"
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
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "All",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "uri": "http://localhost:9998/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    }
                  ],
                  "name": "all",
                  "retention": "P1Y"
                },
                {
                  "description": "The monthly day grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "Day",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "uri": "http://localhost:9998/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    }
                  ],
                  "name": "day",
                  "retention": "P1Y"
                },
                {
                  "description": "The monthly month grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "Month",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "uri": "http://localhost:9998/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    }
                  ],
                  "name": "month",
                  "retention": "P1Y"
                },
                {
                  "description": "The monthly week grain",
                  "dimensions": [
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "Week",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "uri": "http://localhost:9998/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    }
                  ],
                  "name": "week",
                  "retention": "P1Y"
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
                      "uri": "http://localhost:9998/dimensions/breed"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "sex",
                      "name": "sex",
                      "uri": "http://localhost:9998/dimensions/sex"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "species",
                      "name": "species",
                      "uri": "http://localhost:9998/dimensions/species"
                    }
                  ],
                  "longName": "All",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "uri": "http://localhost:9998/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "uri": "http://localhost:9998/metrics/rowNum"
                    }
                  ],
                  "name": "all",
                  "retention": "P1Y"
                },
                {
                  "description": "The pets day grain",
                  "dimensions": [
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "breed",
                      "name": "breed",
                      "uri": "http://localhost:9998/dimensions/breed"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "sex",
                      "name": "sex",
                      "uri": "http://localhost:9998/dimensions/sex"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "species",
                      "name": "species",
                      "uri": "http://localhost:9998/dimensions/species"
                    }
                  ],
                  "longName": "Day",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "uri": "http://localhost:9998/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "uri": "http://localhost:9998/metrics/rowNum"
                    }
                  ],
                  "name": "day",
                  "retention": "P1Y"
                },
                {
                  "description": "The pets month grain",
                  "dimensions": [
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "breed",
                      "name": "breed",
                      "uri": "http://localhost:9998/dimensions/breed"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "sex",
                      "name": "sex",
                      "uri": "http://localhost:9998/dimensions/sex"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "species",
                      "name": "species",
                      "uri": "http://localhost:9998/dimensions/species"
                    }
                  ],
                  "longName": "Month",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "uri": "http://localhost:9998/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "uri": "http://localhost:9998/metrics/rowNum"
                    }
                  ],
                  "name": "month",
                  "retention": "P1Y"
                },
                {
                  "description": "The pets week grain",
                  "dimensions": [
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "breed",
                      "name": "breed",
                      "uri": "http://localhost:9998/dimensions/breed"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "sex",
                      "name": "sex",
                      "uri": "http://localhost:9998/dimensions/sex"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "species",
                      "name": "species",
                      "uri": "http://localhost:9998/dimensions/species"
                    }
                  ],
                  "longName": "Week",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "dayAvgLimbs",
                      "name": "dayAvgLimbs",
                      "uri": "http://localhost:9998/metrics/dayAvgLimbs"
                    },
                    {
                      "category": "General",
                      "longName": "limbs",
                      "name": "limbs",
                      "uri": "http://localhost:9998/metrics/limbs"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "uri": "http://localhost:9998/metrics/rowNum"
                    }
                  ],
                  "name": "week",
                  "retention": "P1Y"
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
                      "uri": "http://localhost:9998/dimensions/color"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "model",
                      "name": "model",
                      "uri": "http://localhost:9998/dimensions/model"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "shape",
                      "name": "shape",
                      "uri": "http://localhost:9998/dimensions/shape"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "size",
                      "name": "size",
                      "uri": "http://localhost:9998/dimensions/size"
                    },
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "All",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "area",
                      "name": "area",
                      "uri": "http://localhost:9998/metrics/area"
                    },
                    {
                      "category": "General",
                      "longName": "booleanMetric",
                      "name": "booleanMetric",
                      "uri": "http://localhost:9998/metrics/booleanMetric"
                    },
                    {
                      "category": "General",
                      "longName": "dayAvgOtherUsers",
                      "name": "dayAvgOtherUsers",
                      "uri": "http://localhost:9998/metrics/dayAvgOtherUsers"
                    },
                    {
                      "category": "General",
                      "longName": "dayAvgUsers",
                      "name": "dayAvgUsers",
                      "uri": "http://localhost:9998/metrics/dayAvgUsers"
                    },
                    {
                      "category": "General",
                      "longName": "depth",
                      "name": "depth",
                      "uri": "http://localhost:9998/metrics/depth"
                    },
                    {
                      "category": "General",
                      "longName": "height",
                      "name": "height",
                      "uri": "http://localhost:9998/metrics/height"
                    },
                    {
                      "category": "General",
                      "longName": "jsonNodeMetric",
                      "name": "jsonNodeMetric",
                      "uri": "http://localhost:9998/metrics/jsonNodeMetric"
                    },
                    {
                      "category": "General",
                      "longName": "nullMetric",
                      "name": "nullMetric",
                      "uri": "http://localhost:9998/metrics/nullMetric"
                    },
                    {
                      "category": "General",
                      "longName": "otherUsers",
                      "name": "otherUsers",
                      "uri": "http://localhost:9998/metrics/otherUsers"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "uri": "http://localhost:9998/metrics/rowNum"
                    },
                    {
                      "category": "General",
                      "longName": "stringMetric",
                      "name": "stringMetric",
                      "uri": "http://localhost:9998/metrics/stringMetric"
                    },
                    {
                      "category": "General",
                      "longName": "users",
                      "name": "users",
                      "uri": "http://localhost:9998/metrics/users"
                    },
                    {
                      "category": "General",
                      "longName": "volume",
                      "name": "volume",
                      "uri": "http://localhost:9998/metrics/volume"
                    },
                    {
                      "category": "General",
                      "longName": "width",
                      "name": "width",
                      "uri": "http://localhost:9998/metrics/width"
                    }
                  ],
                  "name": "all",
                  "retention": "P1Y"
                },
                {
                  "description": "The shapes day grain",
                  "dimensions": [
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "color",
                      "name": "color",
                      "uri": "http://localhost:9998/dimensions/color"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "model",
                      "name": "model",
                      "uri": "http://localhost:9998/dimensions/model"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "shape",
                      "name": "shape",
                      "uri": "http://localhost:9998/dimensions/shape"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "size",
                      "name": "size",
                      "uri": "http://localhost:9998/dimensions/size"
                    },
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "Day",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "area",
                      "name": "area",
                      "uri": "http://localhost:9998/metrics/area"
                    },
                    {
                      "category": "General",
                      "longName": "booleanMetric",
                      "name": "booleanMetric",
                      "uri": "http://localhost:9998/metrics/booleanMetric"
                    },
                    {
                      "category": "General",
                      "longName": "depth",
                      "name": "depth",
                      "uri": "http://localhost:9998/metrics/depth"
                    },
                    {
                      "category": "General",
                      "longName": "height",
                      "name": "height",
                      "uri": "http://localhost:9998/metrics/height"
                    },
                    {
                      "category": "General",
                      "longName": "jsonNodeMetric",
                      "name": "jsonNodeMetric",
                      "uri": "http://localhost:9998/metrics/jsonNodeMetric"
                    },
                    {
                      "category": "General",
                      "longName": "nullMetric",
                      "name": "nullMetric",
                      "uri": "http://localhost:9998/metrics/nullMetric"
                    },
                    {
                      "category": "General",
                      "longName": "otherUsers",
                      "name": "otherUsers",
                      "uri": "http://localhost:9998/metrics/otherUsers"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "uri": "http://localhost:9998/metrics/rowNum"
                    },
                    {
                      "category": "General",
                      "longName": "stringMetric",
                      "name": "stringMetric",
                      "uri": "http://localhost:9998/metrics/stringMetric"
                    },
                    {
                      "category": "General",
                      "longName": "users",
                      "name": "users",
                      "uri": "http://localhost:9998/metrics/users"
                    },
                    {
                      "category": "General",
                      "longName": "volume",
                      "name": "volume",
                      "uri": "http://localhost:9998/metrics/volume"
                    },
                    {
                      "category": "General",
                      "longName": "width",
                      "name": "width",
                      "uri": "http://localhost:9998/metrics/width"
                    }
                  ],
                  "name": "day",
                  "retention": "P1Y"
                },
                {
                  "description": "The shapes month grain",
                  "dimensions": [
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "color",
                      "name": "color",
                      "uri": "http://localhost:9998/dimensions/color"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "model",
                      "name": "model",
                      "uri": "http://localhost:9998/dimensions/model"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "shape",
                      "name": "shape",
                      "uri": "http://localhost:9998/dimensions/shape"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "size",
                      "name": "size",
                      "uri": "http://localhost:9998/dimensions/size"
                    },
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "Month",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "area",
                      "name": "area",
                      "uri": "http://localhost:9998/metrics/area"
                    },
                    {
                      "category": "General",
                      "longName": "booleanMetric",
                      "name": "booleanMetric",
                      "uri": "http://localhost:9998/metrics/booleanMetric"
                    },
                    {
                      "category": "General",
                      "longName": "dayAvgOtherUsers",
                      "name": "dayAvgOtherUsers",
                      "uri": "http://localhost:9998/metrics/dayAvgOtherUsers"
                    },
                    {
                      "category": "General",
                      "longName": "dayAvgUsers",
                      "name": "dayAvgUsers",
                      "uri": "http://localhost:9998/metrics/dayAvgUsers"
                    },
                    {
                      "category": "General",
                      "longName": "depth",
                      "name": "depth",
                      "uri": "http://localhost:9998/metrics/depth"
                    },
                    {
                      "category": "General",
                      "longName": "height",
                      "name": "height",
                      "uri": "http://localhost:9998/metrics/height"
                    },
                    {
                      "category": "General",
                      "longName": "jsonNodeMetric",
                      "name": "jsonNodeMetric",
                      "uri": "http://localhost:9998/metrics/jsonNodeMetric"
                    },
                    {
                      "category": "General",
                      "longName": "nullMetric",
                      "name": "nullMetric",
                      "uri": "http://localhost:9998/metrics/nullMetric"
                    },
                    {
                      "category": "General",
                      "longName": "otherUsers",
                      "name": "otherUsers",
                      "uri": "http://localhost:9998/metrics/otherUsers"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "uri": "http://localhost:9998/metrics/rowNum"
                    },
                    {
                      "category": "General",
                      "longName": "stringMetric",
                      "name": "stringMetric",
                      "uri": "http://localhost:9998/metrics/stringMetric"
                    },
                    {
                      "category": "General",
                      "longName": "users",
                      "name": "users",
                      "uri": "http://localhost:9998/metrics/users"
                    },
                    {
                      "category": "General",
                      "longName": "volume",
                      "name": "volume",
                      "uri": "http://localhost:9998/metrics/volume"
                    },
                    {
                      "category": "General",
                      "longName": "width",
                      "name": "width",
                      "uri": "http://localhost:9998/metrics/width"
                    }
                  ],
                  "name": "month",
                  "retention": "P1Y"
                },
                {
                  "description": "The shapes week grain",
                  "dimensions": [
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "color",
                      "name": "color",
                      "uri": "http://localhost:9998/dimensions/color"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "model",
                      "name": "model",
                      "uri": "http://localhost:9998/dimensions/model"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "shape",
                      "name": "shape",
                      "uri": "http://localhost:9998/dimensions/shape"
                    },
                    {
                      "cardinality": 0,
                      "category": "General",
                      "longName": "size",
                      "name": "size",
                      "uri": "http://localhost:9998/dimensions/size"
                    },
                    {
                      "cardinality": 100000,
                      "category": "General",
                      "longName": "other",
                      "name": "other",
                      "uri": "http://localhost:9998/dimensions/other"
                    }
                  ],
                  "longName": "Week",
                  "metrics": [
                    {
                      "category": "General",
                      "longName": "area",
                      "name": "area",
                      "uri": "http://localhost:9998/metrics/area"
                    },
                    {
                      "category": "General",
                      "longName": "booleanMetric",
                      "name": "booleanMetric",
                      "uri": "http://localhost:9998/metrics/booleanMetric"
                    },
                    {
                      "category": "General",
                      "longName": "dayAvgOtherUsers",
                      "name": "dayAvgOtherUsers",
                      "uri": "http://localhost:9998/metrics/dayAvgOtherUsers"
                    },
                    {
                      "category": "General",
                      "longName": "dayAvgUsers",
                      "name": "dayAvgUsers",
                      "uri": "http://localhost:9998/metrics/dayAvgUsers"
                    },
                    {
                      "category": "General",
                      "longName": "depth",
                      "name": "depth",
                      "uri": "http://localhost:9998/metrics/depth"
                    },
                    {
                      "category": "General",
                      "longName": "height",
                      "name": "height",
                      "uri": "http://localhost:9998/metrics/height"
                    },
                    {
                      "category": "General",
                      "longName": "jsonNodeMetric",
                      "name": "jsonNodeMetric",
                      "uri": "http://localhost:9998/metrics/jsonNodeMetric"
                    },
                    {
                      "category": "General",
                      "longName": "nullMetric",
                      "name": "nullMetric",
                      "uri": "http://localhost:9998/metrics/nullMetric"
                    },
                    {
                      "category": "General",
                      "longName": "otherUsers",
                      "name": "otherUsers",
                      "uri": "http://localhost:9998/metrics/otherUsers"
                    },
                    {
                      "category": "General",
                      "longName": "rowNum",
                      "name": "rowNum",
                      "uri": "http://localhost:9998/metrics/rowNum"
                    },
                    {
                      "category": "General",
                      "longName": "stringMetric",
                      "name": "stringMetric",
                      "uri": "http://localhost:9998/metrics/stringMetric"
                    },
                    {
                      "category": "General",
                      "longName": "users",
                      "name": "users",
                      "uri": "http://localhost:9998/metrics/users"
                    },
                    {
                      "category": "General",
                      "longName": "volume",
                      "name": "volume",
                      "uri": "http://localhost:9998/metrics/volume"
                    },
                    {
                      "category": "General",
                      "longName": "width",
                      "name": "width",
                      "uri": "http://localhost:9998/metrics/width"
                    }
                  ],
                  "name": "week",
                  "retention": "P1Y"
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
