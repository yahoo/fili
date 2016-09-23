// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web
import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.config.ResourceDictionaries
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
        DimensionsApiRequest dimensionsApiRequest
        DimensionsApiRequest mappedDimensionApiRequest
        Exception e

        when:
        dimensionsApiRequest = new DimensionsApiRequest(
                "color",
                "shape|desc-in[shape]",
                null,
                "",
                "",
                resourceDictionaries.getDimensionDictionary(),
                null
        )
        dimensionApiRequestMapper.apply(dimensionsApiRequest, Mock(ContainerRequestContext))

        then:
        e = thrown(BadApiRequestException)
        e.getMessage().matches("Filter.*not match dimension.*")

        when:
        dimensionsApiRequest = new DimensionsApiRequest(
                "color",
                "shape|desc-in[shape],color|desc-in[red]",
                null,
                "",
                "",
                resourceDictionaries.getDimensionDictionary(),
                null
        )
        dimensionApiRequestMapper.apply(dimensionsApiRequest, Mock(ContainerRequestContext))

        then:
        e = thrown(BadApiRequestException)
        e.getMessage().matches("Filter.*not match dimension.*")

        when:
        dimensionsApiRequest = new DimensionsApiRequest(
                "color",
                "color|desc-in[red]",
                null,
                "",
                "",
                resourceDictionaries.getDimensionDictionary(),
                null
        )
        mappedDimensionApiRequest = dimensionApiRequestMapper.apply(dimensionsApiRequest, Mock(ContainerRequestContext))

        then:
        mappedDimensionApiRequest == dimensionsApiRequest
    }
}
