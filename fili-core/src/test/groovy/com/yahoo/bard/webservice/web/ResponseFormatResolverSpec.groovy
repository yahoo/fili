// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import spock.lang.Shared
import spock.lang.Specification

import javax.ws.rs.container.ContainerRequestContext

class ResponseFormatResolverSpec extends Specification {

    @Shared
    ResponseFormatResolver resolver =  [ accept: {format, containerRequestContext ->
            Map<String, String> formatsMap = ["application/json": "json", "application/vnd.api+json": "jsonapi", "text/csv": "csv"]

            String headerFormat = containerRequestContext.getHeaderString("Accept");
            if (format != null || headerFormat == null) {
                return format
            }
            return formatsMap.entrySet().stream()
                    .filter{entry -> headerFormat.contains(entry.getKey())}
                    .map{entry -> entry.getValue()}
                    .findFirst()
                    .orElse(null)
    }] as ResponseFormatResolver

    def requestContextWithAcceptType(String type) {
        return Stub(ContainerRequestContext) {
            getHeaderString("Accept") >> type
        }
    }

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
