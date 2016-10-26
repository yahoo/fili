// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints
import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy
import spock.lang.Specification

import javax.ws.rs.core.Response
/**
 * Test for Jobs endpoint.
 */
class JobsServletSpec extends Specification {
    JerseyTestBinder jtb

    def setup() {
        jtb = new JerseyTestBinder(JobsServlet.class)
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
    }

    def "jobs/ticket endpoint returns the correct response to a get request"() {
        setup:
        String expectedResponse = """{
                "query": "https://localhost:9998/v1/data/QUERY",
                "results": "http://localhost:9998/jobs/ticket1/results",
                "syncResults": "http://localhost:9998/jobs/ticket1/results?asyncAfter=never",
                "self": "http://localhost:9998/jobs/ticket1",
                "status": "success",
                "jobTicket": "ticket1",
                "dateCreated": "2016-01-01",
                "dateUpdated": "2016-01-01",
                "userId": "momo"
        }"""

        when: "We send a request"
        //The buildApiJobStore method in TestBinderFactory sets up the ApiJobStore and stores the metadata for ticket1 in it.
        String result = makeRequest("/jobs/ticket1")

        then: "what we expect"
        GroovyTestUtils.compareJson(result, expectedResponse, JsonSortStrategy.SORT_BOTH)
    }

    def "jobs endpoint returns the number of jobs based on pagination parameters"() {
        setup:
        String expectedResponse = """{
              "jobs": [
                {
                  "dateCreated": "2016-01-01",
                  "dateUpdated": "2016-01-01",
                  "jobTicket": "ticket1",
                  "query": "https://localhost:9998/v1/data/QUERY",
                  "results": "http://localhost:9998/jobs/ticket1/results",
                  "self": "http://localhost:9998/jobs/ticket1",
                  "status": "success",
                  "syncResults": "http://localhost:9998/jobs/ticket1/results?asyncAfter=never",
                  "userId": "momo"
                }
              ],
              "meta": {
                "pagination": {
                  "currentPage": 1,
                  "numberOfResults": 3,
                  "paginationLinks": {
                    "last": "http://localhost:9998/jobs?perPage=1&page=3",
                    "next": "http://localhost:9998/jobs?perPage=1&page=2"
                  },
                  "rowsPerPage": 1
                }
              }
            }"""

        when: "We send a request for the first page of jobs"
        //The buildApiJobStore method in TestBinderFactory sets up the ApiJobStore and stores the metadata for ticket1 in it.
        String result = makeRequest("/jobs", [perPage: [1], page: [1]])

        then: "what we expect is one job row with pagination meta data"
        GroovyTestUtils.compareJson(result, expectedResponse, JsonSortStrategy.SORT_BOTH)
    }

    def "jobs/ticket endpoint returns a 404 if the ticket does not exist in the ApiJobStore"() {
        when:
        Response r = jtb.getHarness().target("/jobs/IDoNotExist").request().get()

        then:
        r.getStatus() == Response.Status.NOT_FOUND.getStatusCode()
        r.readEntity(String.class) == "No job found with job ticket IDoNotExist"
    }

    def "jobs/result endpoint returns a 404 if the ticket does not exist in the ApiJobStore and the PreResponse is not available in the PreResponseStore before timeout"() {
        when: "We send a request to the jobs/IDoNotExist/results endpoint"
        Response r = jtb.getHarness().target("/jobs/IDoNotExist/results").queryParam("asyncAfter", 5).request().get()

        then: "Since the ticket does not exist in the ApiJobStore we get a 404"
        r.getStatus() == Response.Status.NOT_FOUND.getStatusCode()
        r.readEntity(String.class) == "No job found with job ticket IDoNotExist"
    }

    def "jobs/result endpoint returns the result if a ticket does not exist in the ApiJobStore but the PreResponse is available in the PreResponseStore before timeout"() {
        setup:
        String expectedResponse = """{"rows":[{"dateTime":"2016-01-12 00:00:00.000","pageViews":111}]}"""

        when: "We send a request to the jobs/IExistOnlyInPreResponseStore/results endpoint"
        String result = makeRequest("/jobs/IExistOnlyInPreResponseStore/results", [asyncAfter: ["5"]])

        then:
        GroovyTestUtils.compareJson(result, expectedResponse, JsonSortStrategy.SORT_BOTH)
    }

    def "jobs/result endpoint returns the job metadata if the PreResponse is not available in the PreResponsestore in the async timeout"() {
        setup:
        String expectedResponse = """{
                "query": "https://localhost:9998/v1/data/QUERY",
                "results": "http://localhost:9998/jobs/ticket2/results",
                "syncResults": "http://localhost:9998/jobs/ticket2/results?asyncAfter=never",
                "self": "http://localhost:9998/jobs/ticket2",
                "status": "pending",
                "jobTicket": "ticket2",
                "dateCreated": "2016-01-01",
                "dateUpdated": "2016-01-01",
                "userId": "dodo"
        }"""

        when: "We send a request to the jobs/ticket2/results endpoint"
        //The buildApiJobStore method in TestBinderFactory sets up the ApiJobStore and stores the metadata for ticket2 in it.
        String result = makeRequest("/jobs/ticket2/results", [asyncAfter: ["5"]])

        then: "Since the job is not available in the PreResponseStore before the async timeout we get the job payload back"
        GroovyTestUtils.compareJson(result, expectedResponse, JsonSortStrategy.SORT_BOTH)
    }

    def "jobs/result endpoint returns the expected response when the PreResponse is available in the PreResponseStore before async timeout"() {
        setup:
        String expectedResponse = """{"rows":[{"dateTime":"2016-01-12 00:00:00.000","pageViews":111}]}"""

        when: "We send a request to the jobs/ticket1/results endpoint"
        String result = makeRequest("/jobs/ticket1/results", [asyncAfter: ["5"]])

        then:
        GroovyTestUtils.compareJson(result, expectedResponse, JsonSortStrategy.SORT_BOTH)
    }

    def "jobs/result endpoint returns an error if the PreResponse contains an error"() {
        setup:
        String expectedResponse = """{
          "status" : 500,
          "statusName" : "Internal Server Error",
          "reason" : "Error",
          "description" : "Error",
          "druidQuery" : null,
          "requestId": "SOME UUID"
        }"""

        when: "We send a request to the jobs/errorPreResponse/results endpoint"
        Response r = jtb.getHarness().target("/jobs/errorPreResponse/results").request().get()

        then:
        r.getStatus() == 500
        GroovyTestUtils.compareErrorPayload(r.readEntity(String.class), expectedResponse)
    }

    def "/jobs endpoint returns the payload for all the jobs in the ApiJobStore"() {
        setup:
        String expectedResponse = """
               {"jobs":[
                        {
                            "dateCreated":"2016-01-01",
                            "dateUpdated":"2016-01-01",
                            "jobTicket":"ticket1",
                            "query":"https://localhost:9998/v1/data/QUERY",
                            "results":"http://localhost:9998/jobs/ticket1/results",
                            "self":"http://localhost:9998/jobs/ticket1",
                            "status":"success",
                            "syncResults":"http://localhost:9998/jobs/ticket1/results?asyncAfter=never",
                            "userId": "momo"
                        },
                        {
                            "dateCreated":"2016-01-01",
                            "dateUpdated":"2016-01-01",
                            "jobTicket":"ticket2",
                            "query":"https://localhost:9998/v1/data/QUERY",
                            "results":"http://localhost:9998/jobs/ticket2/results",
                            "self":"http://localhost:9998/jobs/ticket2",
                            "status":"pending",
                            "syncResults":"http://localhost:9998/jobs/ticket2/results?asyncAfter=never",
                            "userId": "dodo"
                        },
                        {
                            "dateCreated": "2016-01-01",
                            "dateUpdated":"2016-01-01",
                            "jobTicket": "ticket3p",
                            "query": "https://localhost:9998/v1/data/QUERY",
                            "results": "http://localhost:9998/jobs/ticket3p/results",
                            "self": "http://localhost:9998/jobs/ticket3p",
                            "status": "success",
                            "syncResults": "http://localhost:9998/jobs/ticket3p/results?asyncAfter=never",
                            "userId": "yoyo"
                        }
                      ]
               }"""

        when: "We send a request to the jobs endpoint"
        //The buildApiJobStore method in TestBinderFactory sets up the ApiJobStore and stores the metadata for tickets in it.
        String result = makeRequest("/jobs")

        then: "what we expect"
        GroovyTestUtils.compareJson(result, expectedResponse, JsonSortStrategy.SORT_BOTH)
    }

    def "jobs endpoint returns filtered results when a valid filter is present"() {
        setup:
        String expectedResponse = """{"jobs":[
                                        {
                                            "dateCreated":"2016-01-01",
                                            "dateUpdated":"2016-01-01",
                                            "jobTicket":"ticket1",
                                            "query":"https://localhost:9998/v1/data/QUERY",
                                            "results":"http://localhost:9998/jobs/ticket1/results",
                                            "self":"http://localhost:9998/jobs/ticket1",
                                            "status":"success",
                                            "syncResults":"http://localhost:9998/jobs/ticket1/results?asyncAfter=never",
                                            "userId": "momo"
                                        }
                                  ]}"""

        when: "We send a request to the /jobs endpoint with a valid filters parameter"
        String result = makeRequest("/jobs", [filters: ["userId-eq[momo]"]])

        then: "We only get the job payload that satisfies the filter"
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "jobs endpoint returns an empty list when none of the JobRows satisfy the filter"() {
        when: "We send a request to the /jobs endpoint with a valid filters parameter"
        String result = makeRequest("/jobs", [filters: ["userId-eq[pikachu]"]])

        then: "We only get the job payload that satisfies the filter"
        GroovyTestUtils.compareJson(result, '{"jobs":[]}')
    }

    def "jobs/ticket3p/results returns the number of results we requested through pagination parameters"() {
        when: "We send a request for the first row from the results"
        String result1 = makeRequest("/jobs/ticket3p/results", [asyncAfter: ["5"], perPage: [1], page: [1]])

        then: "We get only first row as we requested against total number of rows"
        GroovyTestUtils.compareJson(result1, getExpectedFirstPage(), JsonSortStrategy.SORT_BOTH)

        when: "We send a request for the last row from the results"
        String result2 = makeRequest("/jobs/ticket3p/results", [asyncAfter: ["5"], perPage: [1], page: [3]])

        then: "We get last row as we requested against total number of rows"
        GroovyTestUtils.compareJson(result2, getExpectedLastPage(), JsonSortStrategy.SORT_BOTH)

        when: "We send a request for the first two rows from the results"
        String result3 = makeRequest("/jobs/ticket3p/results", [asyncAfter: ["5"], perPage: [2], page: [1]])

        then: "We get two rows as we requested against total number of rows"
        GroovyTestUtils.compareJson(result3, getExpectedFirstTwoPages(), JsonSortStrategy.SORT_BOTH)

        when: "We send a request for the middle row from the results"
        String result4 = makeRequest("/jobs/ticket3p/results", [asyncAfter: ["5"], perPage: [1], page: [2]])

        then: "We get middle row as we requested against total number of rows"
        GroovyTestUtils.compareJson(result4, getExpectedMiddlePage(), JsonSortStrategy.SORT_BOTH)
    }

    String makeRequest(String target) {
        makeRequest(target, [:])
    }

    String makeRequest(String target, Map<String, List<String>> queryParams) {
        // Set target of call
        def httpCall = jtb.getHarness().target(target)

        // Add query params to call
        queryParams.each { String key, List<String> values ->
            httpCall = httpCall.queryParam(key, values.join(","))
        }

        // Make the call
        httpCall.request().get(String.class)
    }

    String getExpectedFirstPage() {
        """
            {
              "rows": [
                {
                  "dateTime": "2016-01-12 00:00:00.000",
                  "pageViews": 111
                }
              ],
              "meta": {
                "pagination": {
                  "last": "http://localhost:9998/jobs/ticket3p/results?asyncAfter=5&perPage=1&page=3",
                  "next": "http://localhost:9998/jobs/ticket3p/results?asyncAfter=5&perPage=1&page=2",
                  "currentPage": 1,
                  "rowsPerPage": 1,
                  "numberOfResults": 3
                }
              }
            }
        """
    }

    String getExpectedLastPage() {
        """
            {
              "meta": {
                "pagination": {
                  "currentPage": 3,
                  "first": "http://localhost:9998/jobs/ticket3p/results?asyncAfter=5&perPage=1&page=1",
                  "numberOfResults": 3,
                  "previous": "http://localhost:9998/jobs/ticket3p/results?asyncAfter=5&perPage=1&page=2",
                  "rowsPerPage": 1
                }
              },
              "rows": [
                {
                  "dateTime": "2016-01-14 00:00:00.000",
                  "pageViews": 111
                }
              ]
            }
        """
    }

    String getExpectedFirstTwoPages() {
        """
            {
              "meta": {
                "pagination": {
                  "currentPage": 1,
                  "last": "http://localhost:9998/jobs/ticket3p/results?asyncAfter=5&perPage=2&page=2",
                  "next": "http://localhost:9998/jobs/ticket3p/results?asyncAfter=5&perPage=2&page=2",
                  "numberOfResults": 3,
                  "rowsPerPage": 2
                }
              },
              "rows": [
                {
                  "dateTime": "2016-01-12 00:00:00.000",
                  "pageViews": 111
                },
                {
                  "dateTime": "2016-01-13 00:00:00.000",
                  "pageViews": 111
                }
              ]
            }
         """
    }

    String getExpectedMiddlePage() {
        """
            {
              "rows": [
                {
                  "dateTime": "2016-01-13 00:00:00.000",
                  "pageViews": 111
                }
              ],
              "meta": {
                "pagination": {
                  "first": "http://localhost:9998/jobs/ticket3p/results?asyncAfter=5&perPage=1&page=1",
                  "last": "http://localhost:9998/jobs/ticket3p/results?asyncAfter=5&perPage=1&page=3",
                  "next": "http://localhost:9998/jobs/ticket3p/results?asyncAfter=5&perPage=1&page=3",
                  "previous": "http://localhost:9998/jobs/ticket3p/results?asyncAfter=5&perPage=1&page=1",
                  "currentPage": 2,
                  "rowsPerPage": 1,
                  "numberOfResults": 3
                }
              }
            }
        """
    }
}
