// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.container.ContainerRequestContext

class DefaultResponseFormatResolverSpec extends Specification {

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
        expectedFormat | responseFormat
        null           | resolver.apply(null, requestContextWithAcceptType(null))
        "json"         | resolver.apply("json",requestContextWithAcceptType(null))
        "json"         | resolver.apply(null, requestContextWithAcceptType(DefaultResponseFormatResolver.ACCEPT_HEADER_JSON))
        "csv"          | resolver.apply("csv", requestContextWithAcceptType(null))
        "csv"          | resolver.apply(null, requestContextWithAcceptType(DefaultResponseFormatResolver.ACCEPT_HEADER_CSV))
        "jsonapi"      | resolver.apply(null, requestContextWithAcceptType(DefaultResponseFormatResolver.ACCEPT_HEADER_JSONAPI))
        "csv"          | resolver.apply("csv", requestContextWithAcceptType(DefaultResponseFormatResolver.ACCEPT_HEADER_JSONAPI))
        "json"         | resolver.apply("json", requestContextWithAcceptType(DefaultResponseFormatResolver.ACCEPT_HEADER_CSV))
    }
}
