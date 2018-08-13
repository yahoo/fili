// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.container.ContainerRequestContext

class DefaultResponseUtilsResolverSpec extends Specification {

    @Shared
    ResponseFormatResolver resolver = new DefaultResponseFormatResolver()

    def requestContextWithAcceptType(String type) {
        return Stub(ContainerRequestContext) {
            getHeaderString("Accept") >> type
        }
    }

    @Unroll
    def "test Responseresolver select correct format"() {
        expect:
        responseFormat == expectedFormat

        where:
        expectedFormat                            | responseFormat
        null                                      | resolver.apply(null, requestContextWithAcceptType(null))
        DefaultResponseFormatResolver.URI_JSON    | resolver.apply(DefaultResponseFormatResolver.URI_JSON,requestContextWithAcceptType(null))
        DefaultResponseFormatResolver.URI_JSON    | resolver.apply(null, requestContextWithAcceptType(DefaultResponseFormatResolver.ACCEPT_HEADER_JSON))
        DefaultResponseFormatResolver.URI_CSV     | resolver.apply(DefaultResponseFormatResolver.URI_CSV, requestContextWithAcceptType(null))
        DefaultResponseFormatResolver.URI_CSV     | resolver.apply(null, requestContextWithAcceptType(DefaultResponseFormatResolver.ACCEPT_HEADER_CSV))
        DefaultResponseFormatResolver.URI_JSONAPI | resolver.apply(null, requestContextWithAcceptType(DefaultResponseFormatResolver.ACCEPT_HEADER_JSONAPI))
        DefaultResponseFormatResolver.URI_CSV     | resolver.apply(DefaultResponseFormatResolver.URI_CSV, requestContextWithAcceptType(DefaultResponseFormatResolver.ACCEPT_HEADER_JSONAPI))
        DefaultResponseFormatResolver.URI_JSON    | resolver.apply(DefaultResponseFormatResolver.URI_JSON, requestContextWithAcceptType(DefaultResponseFormatResolver.ACCEPT_HEADER_CSV))
    }
}
