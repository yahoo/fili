// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web
import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.web.apirequest.DimensionsApiRequestImpl
import com.yahoo.bard.webservice.web.endpoints.DimensionsServlet

import spock.lang.Specification

import javax.ws.rs.container.ContainerRequestContext

class DimensionApiRequestMapperSpec extends Specification {
    JerseyTestBinder jtb

    def setup() {
        jtb = new JerseyTestBinder(DimensionsServlet.class)
    }

    def cleanup() {
        jtb.tearDown()
    }

    def "check dimensionApiRequest for filter dimensions not match requested dimensions"() {
        setup:
        ResourceDictionaries resourceDictionaries = jtb.configurationLoader.getDictionaries()
        DimensionApiRequestMapper dimensionApiRequestMapper = new DimensionApiRequestMapper(resourceDictionaries)
        DimensionsApiRequest apiRequest
        DimensionsApiRequest mappedDimensionApiRequest
        Exception e

        when:
        apiRequest = new DimensionsApiRequestImpl(
                "color",
                "shape|desc-in[shape]",
                null,
                "",
                "",
                resourceDictionaries.getDimensionDictionary(),
                null
        )
        dimensionApiRequestMapper.apply(apiRequest, Mock(ContainerRequestContext))

        then:
        e = thrown(BadApiRequestException)
        e.getMessage().matches("Filter.*not match dimension.*")

        when:
        apiRequest = new DimensionsApiRequestImpl(
                "color",
                "shape|desc-in[shape],color|desc-in[red]",
                null,
                "",
                "",
                resourceDictionaries.getDimensionDictionary(),
                null
        )
        dimensionApiRequestMapper.apply(apiRequest, Mock(ContainerRequestContext))

        then:
        e = thrown(BadApiRequestException)
        e.getMessage().matches("Filter.*not match dimension.*")

        when:
        apiRequest = new DimensionsApiRequestImpl(
                "color",
                "color|desc-in[red]",
                null,
                "",
                "",
                resourceDictionaries.getDimensionDictionary(),
                null
        )
        mappedDimensionApiRequest = dimensionApiRequestMapper.apply(apiRequest, Mock(ContainerRequestContext))

        then:
        mappedDimensionApiRequest == apiRequest
    }
}
