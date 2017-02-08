// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.time.GranularityParser
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import spock.lang.Specification
import spock.lang.Timeout

import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.core.Response

@Timeout(30)
// Fail test if hangs
abstract class BaseDataServletComponentSpec extends Specification {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    private static GranularityParser granularityParser = new StandardGranularityParser();

    private static final Logger LOG = LoggerFactory.getLogger(BaseDataServletComponentSpec.class)

    JerseyTestBinder jtb

    abstract Class<?>[] getResourceClasses()

    abstract String getTarget()

    abstract Map<String, List<String>> getQueryParams()

    abstract String getExpectedDruidQuery()

    abstract String getExpectedApiResponse()

    /**
     * Performs any checks necessary on the response headers, such as making sure that links that should be in the
     * header are in the header. By default, this method always assumes the headers are correct.
     *
     * @return True if the headers are as expected, false otherwise.
     */
     boolean headersAreCorrect(MultivaluedMap<String, Object> headers){
        true
    }

    String getFakeDruidResponse() { "[]" }

    Closure<String> getFakeDruidResponseClosure() {
        return { getFakeDruidResponse() }
    }


    def setup() {
        // Create the test web container to test the resources
        jtb = buildTestBinder()

        populatePhysicalTableAvailability()
    }

    /**
     * Used to append headers to API request made by this spec.
     */
    MultivaluedHashMap<String, String> getAdditionalApiRequestHeaders() {
        return [:]
    }

    /**
     * Populates the interval availability of the physical tables.
     * <p>
     * By default, every Physical table believes that it has complete data from January 1st 2010 to December 31 2500.
     */
    void populatePhysicalTableAvailability() {
        AvailabilityTestingUtils.populatePhysicalTableCacheIntervals(jtb, new Interval("2010-01-01/2500-12-31"))
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
    }

    JerseyTestBinder buildTestBinder() {
        new JerseyTestBinder(getResourceClasses())
    }

    void validateJson(String json) {
        MAPPER.readTree(json)
    }

    def "test druid query"() {
        given: "An expected Query"
        validateJson(getExpectedDruidQuery())

        when: "We send a request"
        makeAbstractRequest()

        then: "The query sent to druid is what we expect"
        String jsonQuery
        if (jtb.nonUiDruidWebService instanceof TestDruidWebService) {
            jsonQuery = jtb.nonUiDruidWebService.jsonQuery
        } else {
            jsonQuery = "{}"
        }

        // Most aspects of a Druid query are order-independent, so normalize both lists and maps.
        compareResult(jsonQuery, getExpectedDruidQuery(), JsonSortStrategy.SORT_BOTH)
    }

    @Timeout(10)
    def "test api response"() {
        given: "A known Druid response"
        validateFakeDruidResponse(getFakeDruidResponse())
        validateExpectedApiResponse(getExpectedApiResponse())

        injectDruidResponse(getFakeDruidResponseClosure())

        when: "We send a request"
        Response response = makeAbstractRequest()

        then: "The response headers are what we expect"
        headersAreCorrect(response.getHeaders())

        and: "The response rows are what we expect"
        compareResult(response.readEntity(String), getExpectedApiResponse())
    }

    def validateFakeDruidResponse(String fakeDruidResponse) {
        validateJson(fakeDruidResponse)
    }

    def validateExpectedApiResponse(String expectedApiResponse) {
        validateJson(expectedApiResponse)
    }

    boolean compareResult(String result, String expectedResult, JsonSortStrategy sortStrategy = JsonSortStrategy.SORT_MAPS) {
        GroovyTestUtils.compareJson(result, expectedResult, sortStrategy)
    }

    /**
     *  Injects the fake Druid response into the Druid backend used by the test harness, if
     *  applicable.
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
        if (jtb.nonUiDruidWebService instanceof TestDruidWebService) {
            jtb.nonUiDruidWebService.jsonResponse = druidResponse
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
        def httpCall = jtb.getHarness().target(getTarget())

        // Add query params to call
        queryParams().each { String key, List<String> values ->
            httpCall = httpCall.queryParam(key, values.join(","))
        }

        // Make the call
        Response response = httpCall.request().headers(getAdditionalApiRequestHeaders()).get()
        if (response.status != 200) {
            LOG.error( "***  *** Response status: ${response.status}: ${response.readEntity(String)}")
        }
        response
    }

    public static String getTimeGrainString(String name = "day", String dateTimeZone = "UTC") {
        DateTimeZone timeZone = DateTimeZone.forID(dateTimeZone);
        return MAPPER.writeValueAsString(granularityParser.parseGranularity(name, timeZone))
    }
}
