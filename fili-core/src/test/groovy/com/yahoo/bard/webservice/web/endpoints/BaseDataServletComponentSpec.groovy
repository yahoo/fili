// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.time.GranularityParser
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy

import com.fasterxml.jackson.databind.ObjectMapper

import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import spock.lang.Specification
import spock.lang.Timeout

import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.core.Response

/**
 * This spec serves as a base spec for functional tests against the DataServlet resource. These tests are run in 2
 * phases, the "request" half of request processing, going from an HTTP request to an in-memory version of the application
 * back to a mock Druid client, and then the "response" half of request processing, going from a mock Druid response to
 * an API response.
 */
@Timeout(30)
abstract class BaseDataServletComponentSpec extends Specification {

    private static final Logger LOG = LoggerFactory.getLogger(BaseDataServletComponentSpec.class)
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    private static GranularityParser granularityParser = new StandardGranularityParser()

    JerseyTestBinder jtb

    /**
     * Get the classes for the Jersey resources / Jersey components to have the Jersey test harness load.
     *
     * @return the classes for Jersey to load
     */
    abstract Class<?>[] getResourceClasses()

    /**
     * Get the URL path (just the path segments, starting at the Jersey resource) for the test to request.
     * <p>
     * For example, "data/shapes/week/color".
     *
     * @return the URL path to request for the test
     */
    abstract String getTarget()

    /**
     * Get the query parameters to include in the request for the test.
     *
     * @return the query parameters to add to the request
     */
    abstract Map<String, List<String>> getQueryParams()

    /**
     * Get the expected Druid query that the request will generate, as a String form of JSON.
     *
     * @return the expected Druid query as a JSON string
     */
    abstract String getExpectedDruidQuery()

    /**
     * Get the expected API response that the request will generate, as a String form of JSON.
     *
     * @return the expected API response as a JSON string
     */
    abstract String getExpectedApiResponse()

    /**
     * Get the Druid response to have the test Druid client return when verifying the return portion of the processing
     * cycle is correct. (ie. from Druid response to API response).
     *
     * @return the string representation of the Druid JSON response, defaulting to "[]" (ie. an empty array)
     */
    String getFakeDruidResponse() {
        "[]"
    }

    /**
     * Get a closure that returns the Druid response to have the test Druid client return when verifying the return
     * portion of the processing cycle is correct. (ie. from Druid response to API response).
     *
     * @return a closure returning the string representation of the Druid JSON response, defaulting to "[]" (ie. an
     * empty array)
     */
    Closure<String> getFakeDruidResponseClosure() {
        return { fakeDruidResponse }
    }

    /**
     * Get the headers to include in the request for the test.
     *
     * @return the headers to add to the request
     */
    MultivaluedHashMap<String, String> getAdditionalApiRequestHeaders() {
        [:]
    }

    /**
     * Performs any checks necessary on the response headers, such as making sure that links that should be in the
     * header are in the header. By default, this method always assumes the headers are correct.
     *
     * @return True if the headers are as expected, false otherwise.
     */
     boolean headersAreCorrect(MultivaluedMap<String, Object> headers){
        true
    }

    def setup() {
        jtb = buildTestBinder()

        populatePhysicalTableAvailability()
    }

    def cleanup() {
        jtb.tearDown()
    }

    /**
     * Create the test web container to test the resources.
     *
     * @return the test web container for testing the resources
     */
    JerseyTestBinder buildTestBinder() {
        new JerseyTestBinder(resourceClasses)
    }

    /**
     * Populate the interval availability of the physical table availabilities.
     * <p>
     * By default, every Physical table believes that it has complete data from January 1st 2010 to December 31 2500.
     */
    void populatePhysicalTableAvailability() {
        AvailabilityTestingUtils.populatePhysicalTableCacheIntervals(jtb, new Interval("2010-01-01/2500-12-31"))
    }

    def "test druid query"() {
        given: "An expected Query"
        validateJson(expectedDruidQuery)

        when: "We send a request"
        makeAbstractRequest()

        then: "The query sent to druid is what we expect"
        compareResult(
                jtb.druidWebService instanceof TestDruidWebService ?
                        jtb.druidWebService.jsonQuery :
                        "{}",
                expectedDruidQuery,
                JsonSortStrategy.SORT_BOTH // Most of a Druid query is order-neutral, so normalize both lists and maps.
        )
    }

    @Timeout(10)
    def "test api response"() {
        given: "A known Druid response"
        validateFakeDruidResponse(fakeDruidResponse)
        validateExpectedApiResponse(expectedApiResponse)

        injectDruidResponse(fakeDruidResponseClosure)

        when: "We send a request"
        Response response = makeAbstractRequest()

        then: "The response headers are what we expect"
        headersAreCorrect(response.headers)

        and: "The response rows are what we expect"
        compareResult(response.readEntity(String), expectedApiResponse)
    }

    /**
     * Validate the string is valid JSON.
     *
     * @param json  String representation of JSON to validate
     *
     * @return true if the string is valid JSON
     *
     * @throws RuntimeException if the json string is not valid
     */
    boolean validateJson(String json) {
        MAPPER.readTree(json)
    }

    /**
     * Validate the fake Druid response.
     *
     * @param fakeDruidResponse  String representation of the JSON response to validate
     *
     * @return true if the response is valid
     *
     * @throws RuntimeException if the fake Druid response is not valid
     */
    boolean validateFakeDruidResponse(String fakeDruidResponse) {
        validateJson(fakeDruidResponse)
    }

    /**
     * Validate the expected API response.
     *
     * @param expectedApiResponse  String representation of the JSON response to validate
     *
     * @return true if the response is valid
     *
     * @throws RuntimeException if the expected API response is not valid
     */
    boolean validateExpectedApiResponse(String expectedApiResponse) {
        validateJson(expectedApiResponse)
    }

    /**
     * Compare the given result with the expected result as JSON made from strings, sorting maps by default.
     *
     * @param result  JSON string of the result in question
     * @param expectedResult  JSON string of the expected result
     * @param sortStrategy  Sorting strategy for the JSON contents, defaulting to sorting only maps
     *
     * @return true if the results are the same
     */
    boolean compareResult(
            String result,
            String expectedResult,
            JsonSortStrategy sortStrategy = JsonSortStrategy.SORT_MAPS
    ) {
        GroovyTestUtils.compareJson(result, expectedResult, sortStrategy)
    }

    /**
     *  Injects the fake Druid response into the Druid backend used by the test harness, if applicable.
     *
     *  @param  druidResponse The fake response to be injected.
     */
    void injectDruidResponse(String druidResponse) {
        injectDruidResponse {druidResponse}
    }

    /**
     *  Injects the closure generating the fake Druid response into the Druid backend used by the test harness, if
     *  applicable.
     *
     *  @param  druidResponse The closure generating the fake response to be injected.
     */
    void injectDruidResponse(Closure<String> druidResponse) {
        if (jtb.druidWebService instanceof TestDruidWebService) {
            jtb.druidWebService.jsonResponse = druidResponse
        }
    }

    /*
     Explicitly commented for manual testing.
     */
    //    def "basic stub test"() {
    //        setup:
    //        TestDruidWebService druid = jtb.getDruidWebService();
    //
    //        /* Setup what druid will return */
    //        druid.jsonResponse = "{}"
    //
    //        jtb.getHarness().target("data/network/week/product")
    //                .queryParam("metrics", "dayAvgPageViews")
    //                .queryParam("dateTime", "2014-06-01%2F2014-06-05")
    //                .request()
    //                .get(String.class)
    //
    //        print druid.jsonQuery
    //
    //        expect:
    //        1 == 1
    //    }

    /**
     * Makes a request to the Druid backend.
     *
     * @param queryParams  A zero-argument closure that returns the query parameters as a
     * {@code Map&lt;String, List&lt;String>>} from a query parameter to a list of query parameter values that will be
     * joined by commas. Defaults to {@link BaseDataServletComponentSpec#getQueryParams()} if no argument is provided.
     *
     * @return The response from the Druid backend used by the harness
     */
    Response makeAbstractRequest(Closure queryParams=this.&getQueryParams) {
        // Set target of call
        def httpCall = jtb.harness.target(target)

        // Add query params to call
        queryParams().each { String key, List<String> values ->
            httpCall = httpCall.queryParam(key, values.join(","))
        }

        // Make the call
        Response response = httpCall.request().headers(additionalApiRequestHeaders).get()
        if (response.status != 200) {
            LOG.trace("***  *** Response status: ${response.status}: ${response.readEntity(String)}")
        }
        response
    }

    /**
     * Given a granularity name and a timezone name, parse them into a time grain and serialize it to a String.
     *
     * @param name  Name of the granularity for which to get the time grain, defaults to day.
     * @param dateTimeZone  Name of the timezone for which to get the time grain, defaults to UTC
     *
     * @return the serialized time grain
     */
    public static String getTimeGrainString(String name = "day", String dateTimeZone = "UTC") {
        MAPPER.writeValueAsString(granularityParser.parseGranularity(name, DateTimeZone.forID(dateTimeZone)))
    }
}
