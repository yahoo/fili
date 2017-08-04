package com.yahoo.bard.webservice.async

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils
import com.yahoo.bard.webservice.web.endpoints.BaseDataServletComponentSpec
import com.yahoo.bard.webservice.web.endpoints.DataServlet
import com.yahoo.bard.webservice.web.endpoints.JobsServlet

import com.fasterxml.jackson.databind.ObjectMapper

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Timeout

import javax.validation.constraints.NotNull
import javax.ws.rs.core.Response

/**
 * Serves as a base class for testing asynchronous processes.
 * <p>
 * This test allows us to execute a series of queries one after the other, and use the results of previous queries to
 * build subsequent queries.
 * <p>
 * As such, this spec expects a map of closures rather than a single target. The test will do something similar to, but
 * not quite a reduction over this map. First, it will execute the first closure in the map (note that we are using the
 * fact that Groovy's maps have a well-defined ordering) to get a target. We will make a request to the test server
 * using that target (plus query parameters). Then we will take the result of that query (a Response), verify it, and
 * pass it into the next closure, which will build the next target to make a request against. We will get the result,
 * verify it, and then pass it and the result from the first request to the third closure to get the third target to
 * execute, and so on.
 * <p>
 * The keys of the map are arbitrary, but must be consistent across the maps of target builders, the map of
 * result verifiers, and the map of query parameters within a single specification. Essentially, the keys
 * serve as names for each request.
 * <p>
 * Note that this test is expected to be used for testing asynchronous interactions, _not_ that Bard constructs the
 * correct Druid query for a given request. For tests of that nature, see the {@link BaseDataServletComponentSpec}.
 */
@Timeout(30)
abstract class AsyncFunctionalSpec extends Specification {

    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    JerseyTestBinder jtb

    /**
     * Returns a map of closures, each of which transforms a map of named Responses into request targets.
     * <p>
     * The map keys (i.e. names) are arbitrary, but used to associate targets with query parameters and result
     * validation routines.
     * <p>
     * Each closure is a function Map&ltString, Response&gt -> String that takes a map of named responses, and returns a
     * target to make a request with.
     *
     * @return a map of closures that derives request targets from maps of named query Responses
     */
    abstract Map<String, Closure<String>> getResultsToTargetFunctions()

    /**
     * Returns a map of closures, each of which is an operation Response -> Void that takes a query response, and
     * performs whatever validation on the Response that needs to be performed.
     * <p>
     * The key for each validation must correspond to the key of the target of the associated request.
     *
     * @return a map of closures that take a query result, and performs whatever validation needs to be performed.
     */
    abstract Map<String, Closure<Void>> getResultAssertions()

    /**
     * Get the map of closures generating query parameters for each target.
     * <p>
     * Each closure is a function Map&ltString, Response&gt -> Map&ltString, List&ltString&gt&gt that takes
     * named Responses and returns the query parameters for the associated query.
     * <p>
     * The key for each query parameter generator must correspond to the key of the associated target
     *
     * @return The map of query parameters for each target.
     */
    abstract Map<String, Closure<Map<String, List<String>>>> getQueryParameters()

    /**
     * Returns the classes for the Jetty resources (i.e. servlets) that need to be set up in the test environment.
     *
     * @return An array of classes representing Jetty resources
     */
    Class<?>[] getResourceClasses() {
        return [DataServlet.class, JobsServlet.class]
    }

    /**
     * Returns the generator for the Druid response that should be returned by the backend
     *
     * @return The closure that generates the Druid response that should be returned by the test framework's Druid stub
     */
    abstract Closure<String> getFakeDruidResponse()

    /**
     * Returns the status code that the fake Druid should send when sending results back.
     *
     * @return The status code that the fake Druid should use when sending results back, defaults to 200 (OK)
     */
    int getDruidStatusCode() {
        return 200
    }

    def setup() {
        // Create the test web container to test the resources
        jtb = buildTestBinder()

        populatePhysicalTableAvailability()
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

    @Timeout(10)
    def "The asynchronous workflow executes correctly"() {
        given: "Initialize the backend with the fake Druid response"
        injectDruidResponse(getFakeDruidResponse())

        and: "The map of all previous responses"
        Map<String, Response> previousResponses = [:]

        and: "The closures that perform verification against the results"
        Map<String, Closure<Void>> resultValidations = getResultAssertions()

        and: "The query parameters for each request"
        Map<String, Closure<Map<String, List<String>>>> queryParameters = getQueryParameters()

        expect: "All the asynchronous interactions behave appropriately"
        getResultsToTargetFunctions().each {interactionName, resultsToTarget ->
            //First, we make a request.
            Response response = makeAbstractRequest(
                    // To make a request, we need the target (i.e. path), which may be constructed using responses from
                    // previous requests
                    resultsToTarget(previousResponses),
                    // We also need any query parameters. So we extract the function
                    // <Map, Response> -> Map<String, List<String>> with the same name as the current request-response
                    // cycle from the queryParameters map, and use it to build the query parameters.
                    queryParameters[interactionName](previousResponses)
            )
            response.bufferEntity()
            // Once the response comes back, we need to validate it. So we pull out the appropriate response validation,
            // and pass it the response.
            resultValidations[interactionName](response)
            previousResponses[interactionName] = response
        }
    }

    /**
     *  Injects the closure generating the fake Druid response into the Druid backend used by the test harness, if
     *  applicable.
     *
     *  @param  druidResponse The fake response to be injected.
     */
    def injectDruidResponse(Closure<String> druidResponse) {
        if (jtb.druidWebService instanceof TestDruidWebService) {
            int statusCode = getDruidStatusCode()
            jtb.druidWebService.statusCode = statusCode
            if (statusCode == 200) {
                jtb.druidWebService.jsonResponse = druidResponse
            } else {
                jtb.druidWebService.reasonPhrase = druidResponse.call()
            }
        }
    }

    /**
     * Makes a request to the Druid backend.
     *
     * @param target  The request target
     * @param queryParams The query parameters for the request, must be non-null
     *
     * @return The response from the Druid backend used by the harness
     */
    Response makeAbstractRequest(String target, @NotNull Map<String, List<String>> queryParameters) {
        // Set target of call
        if (queryParameters == null) {
            throw new IllegalArgumentException("Query parameters not found for target $target. Please double-check " +
                    "your map keys.")
        }
        def httpCall = jtb.getHarness().target(target)

        // Add query params to call
        queryParameters.each { String key, List<String> values ->
            httpCall = httpCall.queryParam(key, values.join(","))
        }

        // Make the call
        return httpCall.request().get()
    }
}
