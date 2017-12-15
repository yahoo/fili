// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.util.GroovyTestUtils

import spock.lang.Unroll

import javax.ws.rs.core.HttpHeaders
import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.core.Response

class DataPaginationSpec extends BaseDataServletComponentSpec {

    static final int ROWS_PER_PAGE = 3

    //Total number of rows returned by our fake Druid query
    static final int NUM_ALL_RESULTS = 9

    @Override
    String getFakeDruidResponse() {
        getFakeDruidResponse(NUM_ALL_RESULTS)
    }

    @Override
    Class<?>[] getResourceClasses() {
        return [DataServlet.class]
    }

    @Override
    String getTarget() {
        "data/shapes/day"
    }

    /**
     * Returns query parameters with neither the 'perPage' nor the 'page' fields.
     * @return The query parameters under test
     */
    @Override
    Map<String, List<String>> getQueryParams() {
        [
                metrics: ["height"],
                dateTime: ["2014-09-01/2014-09-13"]
        ]
    }

    /**
     * Returns the expected druid query when not performing pagination.
     *
     * @return The expected druid query when not performing pagination
     */
    @Override
    String getExpectedDruidQuery() {
        //Returns the total page views of each day between September 1 2014, and September 13 2014.
        """{
              "queryType": "timeseries",
              "granularity": ${getTimeGrainString()},
              "intervals": ["2014-09-01T00:00:00.000Z/2014-09-13T00:00:00.000Z"],
              "dataSource" : {
                "name" : "color_shapes",
                "type" : "table"
              },
              "aggregations": [
                    { "name": "height", "fieldName": "height", "type": "longSum" }
              ],
              "postAggregations": [],
              "context": {}
            }"""
    }


    /**
     * Returns the expected API response when not performing pagination
     *
     * @return The expected API response when not performing pagination
     */
    @Override
    String getExpectedApiResponse(){
        getExpectedApiResponse(NUM_ALL_RESULTS, 1, 1, false)
    }

    @Override
    boolean headersAreCorrect(MultivaluedMap<String, Object> headers) {
        headersAreCorrect(headers, NUM_ALL_RESULTS, 1, 1, false)
    }


    /**
     * Returns the hard-coded query parameters plus pagination parameters
     *
     * @param perPage  The number of rows per page desired (may be malformed). If null, then the parameter is not
     * included.
     * @param page  The page number desired (may be malformed). If null, then the parameter is not included
     * @return The query parameters under test
     * @see DataPaginationSpec#getQueryParams
     */
    Map<String, List<String>> getQueryParams(String perPage, String page) {
        Map parameters = getQueryParams()
        if (perPage != null){
            parameters.put("perPage", [perPage])
        }
        if (page != null) {
            parameters.put("page", [page])
        }
        parameters
    }

    /**
     * Generates a Druid response with the following format:
     * [
     *      {
     *          "version" : "v1",
     *          "timestamp": "2014-09-01T00:00:00.000Z",
     *          "event" : {
     *              "height" : 100
     *          }
     *      },
     *      ...
     *      {
     *          "version" : "v1",
     *          "timestamp": "2014-09-09T00:00:00.000Z",
     *          "event" : {
     *              "height" : 100 * n
     *          }
     *      }
     * ]
     *
     * where "n" is the number of rows desired.
     *
     * @param  numRows The number of rows desired.
     * @return Rows of fake data to be paginated
     */
    String getFakeDruidResponse(int numRows) {
        List druidResults = (1..numRows).collect {
            """{
                "timestamp": "${"2014-09-${it.toString().padLeft(2, "0")}T00:00:00.000Z"}",
                "result" : {
                    "height" : ${100 * it}
                }
            }"""
        }
        "[${druidResults.join(',').trim()}]"
    }


    @Unroll
    def "Page #pageParam of #numPages with the correct headers and #rowsPerPage rows per page of data, is returned"() {
        given: "A known Druid response, and the expected API response"
        String druidResponse = getFakeDruidResponse(numPages * rowsPerPage)
        validateJson(druidResponse)
        String apiResponse = getExpectedApiResponse(rowsPerPage, page, numPages, true)
        validateJson(apiResponse)

        injectDruidResponse(druidResponse)

        when: "We send a request"
        Response response = makeAbstractRequest {getQueryParams("$rowsPerPage", pageParam)}

        then:
        headersAreCorrect(response.getHeaders(), ROWS_PER_PAGE, page, numPages, true)
        GroovyTestUtils.compareJson(response.readEntity(String), apiResponse, [])

        where:
        rowsPerPage = ROWS_PER_PAGE
        numPages | page | pageParam
        1        |  1   | "1"
        2        |  1   | "1"
        2        |  2   | "2"
        3        |  1   | "1"
        3        |  2   | "2"
        3        |  3   | "3"
        3        |  1   | "first"
        3        |  3   | "last"
    }

    @Unroll
    def "An error is returned if #page is less than 1 or greater than #numPages"() {
        when: "We send a request with an incorrect page requested"
        Response response = makeAbstractRequest({getQueryParams("$ROWS_PER_PAGE", "$page")})

        then: "We get a 400 (Bad Request) if the page requested is less than 1, and a 404 if past the end"
        response.status == (page < 1 ? 400 : 404)

        where:
        page | numPages
        -512 |  3
        -1   |  3
        0    |  3
        4    |  3
        5    |  3
        512  |  3
    }

    @Unroll
    def "All rows are returned when #perPage rows per page are requested, but there are only #numRequests rows"() {
        given: "A known Druid response, and the expected API response"
        String druidResponse = getFakeDruidResponse(numRequests)
        validateJson(druidResponse)
        String apiResponse = getExpectedApiResponse(perPage, 1, 1, true, NUM_ALL_RESULTS)
        validateJson(apiResponse)

        injectDruidResponse(druidResponse)

        when: "We send a request that returns fewer results than we want per page"
        def foo = getQueryParams("$perPage", "1")

        Response response = makeAbstractRequest({getQueryParams("$perPage", "1")})

        then: "We get all the results in one page, and nothing else"
        headersAreCorrect(response.getHeaders(), perPage, 1, 1, true)
        GroovyTestUtils.compareJson(response.readEntity(String), apiResponse, [])

        where:
        perPage = NUM_ALL_RESULTS + 5
        numRequests = NUM_ALL_RESULTS
    }

    @Unroll
    def "An error is returned if the requested page (#page) or perPage (#perPage) is not a number."() {
        when: "We send a request with an incorrect type of page requested"
        Response response = makeAbstractRequest({getQueryParams("$rowsPerPage", "$page")})

        then: "We get a 400 (bad request) error"
        response.status == 400

        where:
        rowsPerPage   | page
        //Only positive numbers are allowed.
        '1'           | '-2'
        '-2'          | '2'
        '0'           | '2'
        //Only Integers are allowed.
        '2.5'         | '2'
        '1'           | '5.0'
        'a'           | '1'
        '2'           | 'a'
        'a'           | 'a'
        '2a'          | '2'
        '2'           | '2a'
        '2a'          | 'b3'
    }

    def "Making a request without the 'perPage' field but with the 'page' field throws an error"() {
        when: "We send a request with a missing perPage field"
        Response response = makeAbstractRequest {getQueryParams(null, "2")}

        then: "We get a 400 (bad request)"
        response.status == 400
    }

    def "Making a request with the 'perPage' field, but without the 'page' throws an error"(){
        when: "We send a request with only the perPage field"
        Response response = makeAbstractRequest {getQueryParams("2", null)}

        then: "We get a 400 (bad request)"
        response.status == 400
    }

    /**
     * Builds and returns a series of JSON objects representing one page's worth of results.
     *
     * @param perPage The number of rows per page (so the number of JSON objects to return)
     * @param page The page of results to return
     * @return 'perPage' number of JSON objects to page 'page' of results from Druid.
     */
    String buildPage(int perPage, int page){
        //If perPage = 3, then
        //Page 1: [1,3]
        //Page 2: [4,6]
        //Page 3: [7,9]
        int start = page * perPage - perPage + 1
        int end = page * perPage
        List listOfResults = (start..end).collect {
            """{
                "dateTime" : "2014-09-${it.toString().padLeft(2, "0")} 00:00:00.000",
                "height" : ${100 * it}
             }"""
        }
        listOfResults.join(',').trim()
    }


    /**
     * Performs like getExpectedApiResponse, except includes pagination information
     *
     * @param perPage  The number of rows per page. If null, then default to all results, all on one page.
     * @param page  The page desired.
     * @param numPages  The total number of pages that are expected to exist.
     * @param paginating  True if we are paginating. False otherwise.
     * @param perPageToGenerate  The number of rows per page to have in the test data (as opposed to perPage, which is
     * the number of rows per page that are "requested" by the user). These two will not be the same when testing that
     * the case where we have fewer rows of data than the user requests. By default, is equal to the value of perPage.
     * @return The druid query as a JSON
     * @see DataPaginationSpec#getExpectedApiResponse
     */
    String getExpectedApiResponse(int perPage, int page, int numPages, boolean paginating, int perPageToGenerate = 0) {
        perPageToGenerate = perPageToGenerate ?: perPage
        String data = """ "rows" : [
            ${buildPage(perPageToGenerate, page)}
        ]"""

        String first = page != 1 ? /"first": "${buildPageLink(perPage, 1)}"/ : ''
        String last = page != numPages ? /"last": "${buildPageLink(perPage, numPages)}"/ : ''
        String next = page < numPages ? /"next" : "${buildPageLink(perPage, page + 1)}"/ : ''
        String previous = page > 1 ? /"previous" : "${buildPageLink(perPage, page - 1)}"/ : ''
        String metaBlock = ''
        String links = [first, last, next, previous].findAll { it }.join(',')
        if (paginating) {
            metaBlock = /"meta": {
                "pagination": {
                     ${links ? links + ',' : ''}
                     "currentPage": $page,
                     "rowsPerPage": $perPage,
                     "numberOfResults": ${perPageToGenerate * numPages}
                }
            }/
        }

        """{
            $data${paginating ? ", $metaBlock" : ""}
        }"""
    }

    /**
     * Builds a String representation of a link to the specified page of responses
     * @param perPage  The number of rows per page
     * @param page  The page to link to.
     * @return A String representation of a link to the desired page
     */
    String buildPageLink(int perPage, int page){
        "http://localhost:${jtb.getHarness().getPort()}/$target?metrics=height&dateTime=2014-09-01%2F2014-09-13&perPage=$perPage&page=$page"
    }

    /**
     * Verifies that the links to the previous page, next page, first page, and last page of results are as
     * expected.
     * <p>
     * First, this method verifies that the pagination links that should be in the headers are in the header, then it
     * verifies that all the pagination links in the headers should be in the headers.
     *
     * @param headers  The headers to be verified
     * @param perPage  The number of rows per page.
     * @param page  The desired page of results.
     * @param numPages  The total number of pages.
     * @param paginating  True if we are paginating, false otherwise
     * @return True if the links in headers are as expected, false otherwise
     */
    boolean headersAreCorrect(
            MultivaluedMap<String, Object> headers,
            int perPage,
            int page,
            int numPages,
            boolean paginating
    ) {

        //We only have a previous page link if the current page is not the first page
        String prev = page > 1 ? "<${buildPageLink(perPage, page-1)}>" : null

        //We only have a next page link if the current page is not the last page.
        String next = page < numPages ? "<${buildPageLink(perPage, page+1)}>" : null

        String first = page != 1 ?  "<${buildPageLink(perPage, 1)}>" : null
        String last = page != numPages ? "<${buildPageLink(perPage, numPages)}>" : null

        Map<String, String> paginationLinks = ['"first"': first, '"prev"': prev, '"next"': next, '"last"': last]
        Map<String, String> expectedPaginationLinks = [:]
        if (paginating) {
            //We extract all the pagination links that actually have a link associated with them.
            expectedPaginationLinks = paginationLinks.entrySet()
                    .findAll {it.value}
                    .collectEntries()
        }
        Map<String, String> headerLinks = [:]
        if (headers[HttpHeaders.LINK]) {
            //Need to strip the list brackets off of the string representation of headers[HttpHeaders.LINK]
            headerLinks = GroovyTestUtils.splitHeaderLinks((headers[HttpHeaders.LINK] as String)[1..-2])
        }
        boolean expectedLinksPresent = expectedPaginationLinks.keySet().every {
                GroovyTestUtils.compareURL(headerLinks[it], expectedPaginationLinks[it])
        }
        boolean onlyExpectedLinksPresent = headerLinks.keySet().every {
            //If the link is a pagination link, then it should be expected. If the link is not a pagination link
            //then we don't care.
            //Note that we already verified that the URLS are equal when computing expectedLinksPresent.
            paginationLinks.containsKey(it) ? expectedPaginationLinks.containsKey(it) : true
        }
        expectedLinksPresent && onlyExpectedLinksPresent
    }
}
