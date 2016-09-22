// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.MultivaluedHashMap

class RequestContextSpec extends Specification {

    def "Test constructor initializes containers"() {
        setup:
        ContainerRequestContext containerRequestContext = Mock(ContainerRequestContext)
        containerRequestContext.getHeaders() >> (["Bard-Testing": "###BYPASS###", "ClientId": "UI"] as
                MultivaluedHashMap<String, String>)

        RequestContext context = new RequestContext(containerRequestContext, true)

        expect:
        context.containerRequestContext == containerRequestContext
        context.readCache
    }

    @Unroll
    def "Test searchable header for key: #headerKey"() {
        setup:
        ContainerRequestContext containerRequestContext = Mock(ContainerRequestContext)
        containerRequestContext.getHeaders() >> (["Bard-Testing": "###BYPASS###", "ClientId": "UI"] as
                MultivaluedHashMap<String, String>)

        when:
        RequestContext context = new RequestContext(containerRequestContext, true)

        then:
        context.getHeadersLowerCase().getFirst(headerKey) == headerValue

        where:
        headerKey      | headerValue
        "bard-testing" | "###BYPASS###"
        "clientid"     | "UI"
    }
}
