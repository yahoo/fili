// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.apirequest.generator.filter.FilterBinders
import com.yahoo.bard.webservice.web.endpoints.DimensionsServlet

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class DimensionsApiRequestImplSpec extends Specification {

    JerseyTestBinder jtb

    @Shared
    DimensionDictionary fullDictionary

    @Shared
    DimensionDictionary emptyDictionary = new DimensionDictionary()

    FilterBinders filterBinders = FilterBinders.instance

    def setup() {
        jtb = new JerseyTestBinder(DimensionsServlet.class)
        fullDictionary = jtb.configurationLoader.getDimensionDictionary()
    }

    def cleanup() {
        jtb.tearDown()
    }

    def "check api request construction for the top level endpoint (all dimensions)"() {
        when:
        DimensionsApiRequestImpl apiRequest = new DimensionsApiRequestImpl(
                null, // dimensions
                null, // filters
                null,  //format
                "",  // Per Page
                "",   // Page
                fullDictionary
        )

        then:
        apiRequest.getDimensions() as Set == fullDictionary.findAll() as Set
    }

    def "check api request construction for a given dimension name"() {
        setup:
        String name = "color"

        when:
        DimensionsApiRequestImpl apiRequest = new DimensionsApiRequestImpl(
                name,
                null, // filters
                null, // format
                "",  // Per page
                "",  // Page
                fullDictionary
        )

        then:
        apiRequest.getDimension() == fullDictionary.findByApiName(name)
    }

    def "check api request construction for a given dimension name and a given filter"() {
        setup:
        String name = "color"
        String filterString = "color|desc-in[color]"
        ApiFilter expectedFilter = filterBinders.generateApiFilter(filterString, fullDictionary)

        when:
        DimensionsApiRequestImpl apiRequest = new DimensionsApiRequestImpl(
                name,
                filterString,
                null,       //format
                "",       //perPAge
                "",          // Page
                fullDictionary
        )

        then:
        apiRequest.getDimension() == fullDictionary.findByApiName(name)
        apiRequest.getFilters() == [expectedFilter] as LinkedHashSet
    }

    @Unroll
    def "api request construction throws #exception.simpleName because #reason"() {
        when:
        new DimensionsApiRequestImpl(
                name,
                filter,
                null,   // format
                "",   // per page
                "", // page
                dictionary
        )

        then:
        Exception e = thrown(exception)
        e.getMessage().matches(reason)

        where:
        name    | filter                 | dictionary      | exception              | reason
        "color" | "color|desc-in[color]" | emptyDictionary | BadApiRequestException | "Dimension Dictionary is empty.*"
        "blank" | "color|desc-in[color]" | fullDictionary  | BadApiRequestException | "Dimension.* do not exist.*"
        null    | "blank|desc-in[blank]" | fullDictionary  | BadApiRequestException | "Filter.*does not exist.*"
        //More enhanced dimension dictionary is needed to test what happens when a filter matches multiple dimensions
        //null  | "shape|desc-in[shape]" | fullDictionary  | BadApiRequestException | "Filter.*not match dimension.*"
    }
}
