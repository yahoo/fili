// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.inject.Inject
import javax.ws.rs.container.ContainerRequestContext

class ResponseFormatResolverSpec extends Specification {

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
        null           | resolver.accept(null, requestContextWithAcceptType(null))
        "json"         | resolver.accept("json",requestContextWithAcceptType(null))
        "json"         | resolver.accept(null, requestContextWithAcceptType("application/json"))
        "csv"          | resolver.accept("csv", requestContextWithAcceptType(null))
        "csv"          | resolver.accept(null, requestContextWithAcceptType("text/csv"))
        "jsonapi"      | resolver.accept(null, requestContextWithAcceptType("application/vnd.api+json"))
        "csv"          | resolver.accept("csv", requestContextWithAcceptType("application/vnd.api+json"))
        "json"         | resolver.accept("json", requestContextWithAcceptType("text/csv"))
    }
}
