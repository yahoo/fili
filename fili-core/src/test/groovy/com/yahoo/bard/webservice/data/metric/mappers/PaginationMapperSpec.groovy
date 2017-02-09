// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.Result
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.ResultSetSchema
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.dimension.KeyValueStore
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProviderManager
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.web.DataApiRequest
import com.yahoo.bard.webservice.web.PageNotFoundException
import com.yahoo.bard.webservice.web.responseprocessors.LoggingContext
import com.yahoo.bard.webservice.web.responseprocessors.MappingResponseProcessor
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys
import com.yahoo.bard.webservice.web.util.PaginationLink
import com.yahoo.bard.webservice.web.util.PaginationParameters

import com.fasterxml.jackson.databind.JsonNode

import org.joda.time.DateTime

import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.HttpHeaders
import javax.ws.rs.core.Link
import javax.ws.rs.core.Response
import javax.ws.rs.core.UriBuilder

class PaginationMapperSpec extends Specification {

    private KeyValueStore store
    private SearchProvider searchProvider
    private MappingResponseProcessor responseProcessor
    private UriBuilder uriBuilder
    private DataApiRequest apiRequest
    private ObjectMappersSuite objectMappers

    def setup(){
        apiRequest = Mock(DataApiRequest)
        apiRequest.getLogicalMetrics() >> Collections.emptySet()
        objectMappers = new ObjectMappersSuite()
        store = MapStoreManager.getInstance("testStore")
        searchProvider = NoOpSearchProviderManager.getInstance("testSearchProvider")
        responseProcessor = new MappingResponseProcessor(apiRequest, objectMappers) {
            @Override
            FailureCallback getFailureCallback(DruidAggregationQuery<?> query) {
                return null
            }

            @Override
            HttpErrorCallback getErrorCallback(DruidAggregationQuery<?> query) {
                return null
            }

            @Override
            void processResponse(JsonNode json, DruidAggregationQuery<?> query, LoggingContext metadata) {}
        }
        uriBuilder = UriBuilder.fromUri("""http://example.yahoo.com:1234/network/day/platform?metrics=pageViews&
                perPage=1&page=1""".replaceAll("\\s", ""))
    }

    @Unroll
    def "Returns page #page of #numPages pages with #rowsPerPage rows per page, adds the links #links"() {
        given: "An appropriate uriBuilder"
        uriBuilder = UriBuilder.fromUri(
                """http://example.yahoo.com:1234/network/day/platform?metrics=pageViews&
                perPage=$rowsPerPage&page=$page""".replaceAll("\\s", "")
        )

        and: "The test result set, expected subset of results, and desired page of data"
        ResultSet testResults = buildResultSet(numPages * rowsPerPage)
        ResultSet expectedData = buildExpectedPage(testResults, page, rowsPerPage)
        PaginationMapper mapper = new PaginationMapper(
                new PaginationParameters(rowsPerPage, page),
                responseProcessor,
                uriBuilder
        )

        and: "The expected links in the headers and body of the response"
        UriBuilder first = UriBuilder.fromUri(firstPage)
        UriBuilder last = UriBuilder.fromUri(lastPage)

        Map<String, URI> expectedBodyLinks = [first: first.build(), last: last.build()]
        List<Link> expectedHeaders = null

        if (page != 1) {
            expectedHeaders = expectedHeaders ?: []
            expectedHeaders.add(Link.fromUriBuilder(first).rel(PaginationLink.FIRST.getHeaderName()).build())
        }

        if (page != numPages) {
            expectedHeaders = expectedHeaders ?: []
            expectedHeaders.add(Link.fromUriBuilder(last).rel(PaginationLink.LAST.getHeaderName()).build())
        }

        if (page < numPages) {
            UriBuilder nextPageLink = UriBuilder.fromUri(nextPage)
            expectedHeaders.add(Link.fromUriBuilder(nextPageLink).rel(PaginationLink.NEXT.getHeaderName()).build())
            expectedBodyLinks.put(PaginationLink.NEXT.getBodyName(), nextPageLink.build())
            links["next"] = nextPage

        }

        if (1 < page) {
            UriBuilder prevPageLink = UriBuilder.fromUri(prevPage)
            expectedHeaders.add(Link.fromUriBuilder(prevPageLink).rel(PaginationLink.PREVIOUS.getHeaderName()).build())
            expectedBodyLinks.put(PaginationLink.PREVIOUS.getBodyName(), prevPageLink.build())
            links["prev"] = prevPage
        }

        when: "We build the page of data"
        ResultSet pageOfData = mapper.map(testResults)

        then: "We expect the data to be correct, the links to be added properly, and the pagination to be there"
        pageOfData == expectedData
        List<Link> computedHeaders = responseProcessor.getHeaders()[HttpHeaders.LINK] as List<Link>
        //Apparently, Link's equality is not order agnostic }:-(.
        computedHeaders == expectedHeaders || [computedHeaders, expectedHeaders].transpose()
                .every { computed, expected -> GroovyTestUtils.compareURL(computed.toString(), expected.toString()) }

        Map<String, URI> bodyLinks = responseProcessor
                .getResponseContext()[ResponseContextKeys.PAGINATION_LINKS_CONTEXT_KEY.getName()] as Map<String, URI>

        //Apparently URLs don't actually test for equality in a parameter order-agnostic fashion? Because...reasons?
        bodyLinks.keySet().every {
            GroovyTestUtils.compareURL(bodyLinks[it].toString(), expectedBodyLinks[it].toString())
        }

        responseProcessor.getResponseContext().get(ResponseContextKeys.PAGINATION_CONTEXT_KEY.getName())

        where:
        page | numPages | rowsPerPage
        1    | 1        | 1
        1    | 1        | 3
        1    | 2        | 1
        1    | 2        | 3
        2    | 2        | 1
        2    | 2        | 3
        2    | 3        | 3
        3    | 4        | 3

        firstPage = """http://example.yahoo.com:1234/network/day/platform?metrics=pageViews&
                perPage=$rowsPerPage&page=1""".replaceAll("\\s", "")
        lastPage = """http://example.yahoo.com:1234/network/day/platform?metrics=pageViews&
                perPage=$rowsPerPage&page=$numPages""".replaceAll("\\s", "")
        nextPage = """http://example.yahoo.com:1234/network/day/platform?metrics=pageViews&
                perPage=$rowsPerPage&page=${page+1}""".replaceAll("\\s", "")
        prevPage = """http://example.yahoo.com:1234/network/day/platform?metrics=pageViews&
                perPage=$rowsPerPage&page=${page-1}""".replaceAll("\\s", "")

        links = [first:firstPage, last:lastPage]
    }


    @Unroll
    def "Get #numLastRows not #rowsPerPage rows when asking for page #lastPage, and last page has #numLastRows rows"() {
        given: "The test results and mapper"
        ResultSet testResults = buildResultSet(lastFullPage * rowsPerPage + numLastRows)
        PaginationMapper mapper = new PaginationMapper(
                new PaginationParameters(rowsPerPage, lastPage),
                responseProcessor,
                uriBuilder
        )
        and: "The expected data"
        ResultSet expectedPage = new ResultSet(
                testResults.getSchema(),
                testResults[-numLastRows..-1]
        )

        expect: "The computed subset of results is as expected"
        mapper.map(testResults) == expectedPage

        where:
        lastFullPage | rowsPerPage | numLastRows
        1            | 3           | 1
        3            | 3           | 2
        lastPage = lastFullPage + 1

    }

    @Unroll
    def "An exception is thrown when desired page is #page but last page is #numPages"() {
        given: "A pagination mapper with a desired page past the last."
        PaginationMapper paginator = new PaginationMapper(
                new PaginationParameters(rowsPerPage, page),
                responseProcessor,
                uriBuilder
        )
        ResultSet testResults = buildResultSet(numPages * rowsPerPage)

        when: "We attempt to extract the desired page"
        paginator.map(testResults)

        then:
        def exception = thrown(PageNotFoundException)
        exception.getMessage() == getExpectedErrorMessage(page, rowsPerPage, numPages)
        exception.getErrorStatus() == Response.Status.NOT_FOUND

        where:
        page | rowsPerPage | numPages
        2    | 3           | 1
        3    | 3           | 1
        3    | 3           | 2
    }


    String getExpectedErrorMessage(int page, int rowsPerPage, int numPages) {
        "Requested page '$page' with '$rowsPerPage' rows per page, but there are only '$numPages' pages."
    }


    /**
     * Build a set of dummy results.
     *
     * @param numRows  The number of rows of results to build.
     * @return The dummy result set
     */
    ResultSet buildResultSet(int numRows)
    {
        int numDimensions = 3
        int numMetrics = 3
        List resultList = (1..numRows).collect(){rowNum ->
            Map dimensionData =buildDimensionData(numDimensions)

            Map metricValues = (1..numMetrics).collectEntries(){
                MetricColumn metricColumn = new MetricColumn("metric$it")
                [(metricColumn): it * rowNum]
            }

            new Result(dimensionData, metricValues, new DateTime())
        }
        new ResultSet(new ResultSetSchema(DAY, [].toSet()), resultList)

    }

    /**
     * Given a result set, returns the desired page.
     * @param results  The results to extract data from.
     * @param page  The page of data desired.
     * @param perPage  The number of rows per page desired.
     * @return The desired page of results
     */
    ResultSet buildExpectedPage(ResultSet results, int page, int perPage) {
        return new ResultSet(
                results.getSchema(),
                results.subList((page - 1) * perPage, page * perPage)
        )
    }

    /**
     * Build test dimension data.
     * @param numDimensions  The number of dimensions to build tastes for.
     * @return The test dimension data
     */
    Map buildDimensionData(int numDimensions) {
        (1..numDimensions).collectEntries() {
            Dimension dimension = new KeyValueStoreDimension(
                    "dimension$it" as String,
                    "dimension$it" as String,
                    new LinkedHashSet<DimensionField>([BardDimensionField.ID, BardDimensionField.DESC]),
                    store,
                    searchProvider
            )
            DimensionColumn column = new DimensionColumn(dimension)
            DimensionField idField = BardDimensionField.ID
            DimensionField descField = BardDimensionField.DESC
            Map<DimensionField, String> fieldValueMap = [
                    (idField): "dimension$it" as String,
                    (descField): "dimension$it" as String
            ]
            [(column): new DimensionRow(idField, fieldValueMap)]
        }
    }
}
