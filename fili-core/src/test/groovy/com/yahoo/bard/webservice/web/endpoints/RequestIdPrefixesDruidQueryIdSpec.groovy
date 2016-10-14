// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.util.JsonSlurper
import com.yahoo.bard.webservice.util.JsonSortStrategy
import com.yahoo.bard.webservice.web.filters.BardLoggingFilter
import org.apache.commons.lang.StringUtils

import javax.ws.rs.core.MultivaluedHashMap


class RequestIdPrefixesDruidQueryIdSpec extends BaseDataServletComponentSpec {
    def prefixId = UUID.randomUUID().toString();

    @Override
    def setup() {
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        dimensionStore.findByApiName("color").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Foo", "FooDesc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Bar", "BarDesc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Baz", "BazDesc"))
        }
    }

    @Override
    Class<?>[] getResourceClasses() {
        [ DataServlet.class, BardLoggingFilter.class ]
    }

    @Override
    String getTarget() {
        return "data/shapes/week/color"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics": ["width","depth"],
                "dateTime": ["2014-06-02/2014-06-30"],
                "sort": ["width|desc","depth|asc"]
        ]
    }

    @Override
    boolean compareResult(String result, String expectedResult, JsonSortStrategy sortStrategy = JsonSortStrategy.SORT_MAPS) {
        def parsedJson = new JsonSlurper(sortStrategy).parseText(result)
        if (parsedJson.context != null) {
            return parsedJson.context.queryId.startsWith(prefixId)
        }
        // Default to true-- the base spec runs an extra test which we consider a noop.
        return true
    }

    @Override
    String getExpectedDruidQuery() {
        // Noop
        """{}"""
    }

    @Override
    String getFakeDruidResponse() {
        // Noop
        """[]"""
    }

    @Override
    String getExpectedApiResponse() {
        // Noop
        """{}"""
    }

    @Override
    MultivaluedHashMap<String, String> getAdditionalApiRequestHeaders() {
        return ["x-request-id": prefixId]
    }

    def "verify invalid x-request-id values"() {
        BardLoggingFilter filter = new BardLoggingFilter()
        assert !filter.isValidRequestId('abcd$') // Invalid char
        assert !filter.isValidRequestId(StringUtils.leftPad('a', 200, 'a')) // Too long
        assert !filter.isValidRequestId('') // empty string
        assert !filter.isValidRequestId(null) // null
    }
}
