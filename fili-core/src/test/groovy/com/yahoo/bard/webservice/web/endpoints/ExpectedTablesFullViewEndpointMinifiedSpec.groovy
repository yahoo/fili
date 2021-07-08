// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
 package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.logging.TestLogAppender
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.web.LoggingTestUtils
import com.yahoo.bard.webservice.web.filters.BardLoggingFilter

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Shared

/**
 * Minified version of the fullview endpoint test that contains only the hourly and pets tables.
 *
 * The motivation behind this test is that occasionally when the TablesFullViewEndpoint test fails, building the power
 * assertions causes and OutOfMemoryError, and the error logs are lost. This makes debugging errors that that test catches
 * difficult. Hopefully, most errors that would affect the TablesFullViewEndpoint test will also cause this test to fail,
 * and the error logs will NOT be lost.
 */
class ExpectedTablesFullViewEndpointMinifiedSpec extends BaseTableServletComponentSpec {

    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    @Shared
    TestLogAppender logAppender

    def setupSpec() {
        // Hook with test appender
        logAppender = new TestLogAppender()
    }


    def cleanupSpec() {
        logAppender.close()
    }

    def setup() {

        TableIdentifier hourlyAllTableIdentifier = new TableIdentifier("hourly", null)
        TableIdentifier hourlyHourTableIdentifier = new TableIdentifier("hourly", DefaultTimeGrain.HOUR)
        TableIdentifier hourlyDayTableIdentifier = new TableIdentifier("hourly", DefaultTimeGrain.DAY)
        TableIdentifier hourlyWeekTableIdentifier = new TableIdentifier("hourly", DefaultTimeGrain.WEEK)
        TableIdentifier hourlyMonthTableIdentifier = new TableIdentifier("hourly", DefaultTimeGrain.MONTH)

        LogicalTable hourlyAllTable = jtb.getConfigurationLoader().getLogicalTableDictionary().get(hourlyAllTableIdentifier)
        LogicalTable hourlyHourTable = jtb.getConfigurationLoader().getLogicalTableDictionary().get(hourlyHourTableIdentifier)
        LogicalTable hourlyDayTable = jtb.getConfigurationLoader().getLogicalTableDictionary().get(hourlyDayTableIdentifier)
        LogicalTable hourlyWeekTable = jtb.getConfigurationLoader().getLogicalTableDictionary().get(hourlyWeekTableIdentifier)
        LogicalTable hourlyMonthTable = jtb.getConfigurationLoader().getLogicalTableDictionary().get(hourlyMonthTableIdentifier)

        TableIdentifier petsAllTableIdentifier = new TableIdentifier("pets", null)
        TableIdentifier petsDayTableIdentifier = new TableIdentifier("pets", DefaultTimeGrain.DAY)
        TableIdentifier petsWeekTableIdentifier = new TableIdentifier("pets", DefaultTimeGrain.WEEK)
        TableIdentifier petsMonthTableIdentifier = new TableIdentifier("pets", DefaultTimeGrain.MONTH)

        LogicalTable petsAllTable = jtb.getConfigurationLoader().getLogicalTableDictionary().get(petsAllTableIdentifier)
        LogicalTable petsDayTable = jtb.getConfigurationLoader().getLogicalTableDictionary().get(petsDayTableIdentifier)
        LogicalTable petsWeekTable = jtb.getConfigurationLoader().getLogicalTableDictionary().get(petsWeekTableIdentifier)
        LogicalTable petsMonthTable = jtb.getConfigurationLoader().getLogicalTableDictionary().get(petsMonthTableIdentifier)

        jtb.getConfigurationLoader().getLogicalTableDictionary().clear()
        jtb.getConfigurationLoader().getLogicalTableDictionary().put(hourlyHourTableIdentifier, hourlyHourTable)
        jtb.getConfigurationLoader().getLogicalTableDictionary().put(hourlyDayTableIdentifier, hourlyDayTable)
        jtb.getConfigurationLoader().getLogicalTableDictionary().put(hourlyWeekTableIdentifier, hourlyWeekTable)
        jtb.getConfigurationLoader().getLogicalTableDictionary().put(hourlyMonthTableIdentifier, hourlyMonthTable)
        jtb.getConfigurationLoader().getLogicalTableDictionary().put(hourlyAllTableIdentifier, hourlyAllTable)

        jtb.getConfigurationLoader().getLogicalTableDictionary().put(petsAllTableIdentifier, petsAllTable)
        jtb.getConfigurationLoader().getLogicalTableDictionary().put(petsDayTableIdentifier, petsDayTable)
        jtb.getConfigurationLoader().getLogicalTableDictionary().put(petsWeekTableIdentifier, petsWeekTable)
        jtb.getConfigurationLoader().getLogicalTableDictionary().put(petsMonthTableIdentifier, petsMonthTable)
    }

    def cleanup() {
        logAppender.clear()
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
