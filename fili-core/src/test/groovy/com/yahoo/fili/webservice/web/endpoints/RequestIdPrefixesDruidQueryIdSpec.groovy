// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.web.endpoints

import com.yahoo.fili.webservice.data.dimension.DimensionDictionary
import com.yahoo.fili.webservice.data.dimension.FiliDimensionField
import com.yahoo.fili.webservice.util.JsonSlurper
import com.yahoo.fili.webservice.util.JsonSortStrategy
import com.yahoo.fili.webservice.web.filters.FiliLoggingFilter
import org.apache.commons.lang.StringUtils

import javax.ws.rs.core.MultivaluedHashMap


class RequestIdPrefixesDruidQueryIdSpec extends BaseDataServletComponentSpec {
    def prefixId = UUID.randomUUID().toString();

    @Override
    def setup() {
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        dimensionStore.findByApiName("color").with {
            addDimensionRow(FiliDimensionField.makeDimensionRow(it, "Foo", "FooDesc"))
            addDimensionRow(FiliDimensionField.makeDimensionRow(it, "Bar", "BarDesc"))
            addDimensionRow(FiliDimensionField.makeDimensionRow(it, "Baz", "BazDesc"))
        }
    }

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class, FiliLoggingFilter.class ]
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
        FiliLoggingFilter filter = new FiliLoggingFilter()
        assert !filter.isValidRequestId('abcd$') // Invalid char
        assert !filter.isValidRequestId(StringUtils.leftPad('a', 200, 'a')) // Too long
        assert !filter.isValidRequestId('') // empty string
        assert !filter.isValidRequestId(null) // null
    }
}
