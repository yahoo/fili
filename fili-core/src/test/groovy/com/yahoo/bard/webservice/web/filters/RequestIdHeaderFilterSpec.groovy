// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters

import spock.lang.Specification

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.container.ContainerResponseContext
import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.MultivaluedMap

/**
 * Tests that X-Request-ID shows up in the header
 */
class RequestIdHeaderFilterSpec extends Specification {
    def "verify that RequestIdHeaderFilter adds the request id to the response headers"() {
        setup: "stub for request context"
        MultivaluedMap<String, String> requestHeaders = new MultivaluedHashMap<>()
        ContainerRequestContext request = Mock(ContainerRequestContext)
        request.getHeaders() >> requestHeaders

        and: "stub for response context"
        MultivaluedMap<String, String> responseHeaders = new MultivaluedHashMap<>()
        ContainerResponseContext response = Mock(ContainerResponseContext)
        response.getHeaders() >> responseHeaders

        RequestIdHeaderFilter requestIdHeaderFilter = new RequestIdHeaderFilter()

        when:
        requestIdHeaderFilter.filter(request, response)

        then:
        responseHeaders.containsKey(RequestIdHeaderFilter.X_REQUEST_ID)
    }
}
