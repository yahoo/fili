// Copyright 2019, Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.table.availability.AvailabilityTestingUtils
import com.yahoo.bard.webservice.util.SinglePagePagination
import com.yahoo.bard.webservice.web.util.PaginationParameters
import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.config.names.TestDimensionSearchLogicalTableName
import com.yahoo.bard.webservice.data.dimension.SearchQuerySearchProvider

import org.joda.time.Interval

import spock.lang.Specification

import javax.ws.rs.NotAllowedException

class DimensionSearchServletSpec extends Specification {
    JerseyTestBinder jtb

    def setup() {
        // Create the test web container to test the resources
        jtb = new JerseyTestBinder(DimensionSearchServlet.class)
        AvailabilityTestingUtils.populatePhysicalTableCacheIntervals(
                jtb,
                new Interval("2018-01-01/2018-02-01"),
                [TestDimensionSearchLogicalTableName.TABLE.asName()] as Set
        )
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
    }

    String makeRequest(String target, queryParams=[:] as Map<String, List<String>>) {
        // Set target of call
        def httpCall = jtb.getHarness().target(target)
        queryParams.each {
            String key, List<String> values ->
                httpCall = httpCall.queryParam(key, values.join(','))
        }

        // Make the call
        httpCall.request().get(String.class)
    }

    def "making a search request against a dimension that uses a SearchQuerySearchProvider will correctly utilize the search query"() {
        setup:
        SearchQuerySearchProvider searchQuerySearchProvider = Mock()
        Dimension dim = Spy(jtb.configurationLoader.dimensionDictionary.findAll().iterator().next())
        dim.getSearchProvider() >> searchQuerySearchProvider
        jtb.configurationLoader.dimensionDictionary.@apiNameToDimension.put(dim.getApiName(), dim)

        when:
        makeRequest("/dimensions/${dim.getApiName()}/search", ["query" : ["queryString"]] as Map)

        then:
        /*  We only care that the search query search provider is correctly called with the query string
            However, the test will fail if null is returned. Instead of silently catching the error we prefer to
            return a simple response and let the call finish gracefully.
        */
        1 * searchQuerySearchProvider.findSearchRowsPaged("queryString", _ as PaginationParameters) >> new SinglePagePagination<>([], PaginationParameters.EVERYTHING_IN_ONE_PAGE, 0)
    }

    def "making a search request against a dimension that does NOT use SearchQuerySearchProvider will throw an error indicating search is not supported on the provided dimension"() {
        setup:
        SearchProvider searchProvider = Mock()
        Dimension dim = Spy(jtb.configurationLoader.dimensionDictionary.findAll().iterator().next())
        dim.getSearchProvider() >> searchProvider
        jtb.configurationLoader.dimensionDictionary.@apiNameToDimension.put(dim.getApiName(), dim)

        when:
        makeRequest("/dimensions/${dim.getApiName()}/search", ["query" : ["queryString"]] as Map)

        then:
        thrown(NotAllowedException)
    }
}
