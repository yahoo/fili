// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters

import com.yahoo.bard.webservice.logging.RequestLog

import org.slf4j.MDC

import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Unroll

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.UriInfo

class BardLoggingFilterSpec extends Specification {

    @Subject BardLoggingFilter filter = new BardLoggingFilter()

    MultivaluedHashMap<String, String> fakeHeaders = new MultivaluedHashMap<>()
    ContainerRequestContext requestContext = Stub(ContainerRequestContext) {
            getHeaders() >> fakeHeaders
            getUriInfo() >> Stub(UriInfo) {
                getRequestUri() >> new URI(
                        "https://bard.bard.com/v1/public/table/grain?metrics=blah&dateTime=2017-01-01/2018-01-01"
                )
            }
    }

    def cleanup() {
        MDC.remove(RequestLog.ID_KEY)
        fakeHeaders.clear()
    }

    @Unroll
    def "The BardLoggingFilter successfully adds #requestId to MDC"() {
        given: "A ContainerRequestContext with the specified requestId"
        fakeHeaders.put(BardLoggingFilter.X_REQUEST_ID_HEADER, [requestId])

        when:
        filter.filter(requestContext)

        then:
        MDC.get(RequestLog.ID_KEY).startsWith(requestId)

        where:
        requestId << [
                "5",
                "a",
                "requestId",
                "5requestId",
                "a",
                "5",
                "5".multiply(200),
                "5+a-b=6",
                "5_little_monkeys"
        ]
    }

    def "The BardLoggingFilter is null safe"() {
        given: "A ContainerRequestContext with a null requestId"
        fakeHeaders.put(BardLoggingFilter.X_REQUEST_ID_HEADER, [null])

        when:
        filter.filter(requestContext)

        then:
        notThrown(NullPointerException)
    }

    @Unroll
    def "The BardLoggingFilter does not add a malformed requestId '#requestId' to MDC"() {
        given: "A ContainerRequestContext with the specified malformed requestId"
        fakeHeaders.put(BardLoggingFilter.X_REQUEST_ID_HEADER, [requestId])

        when:
        filter.filter(requestContext)

        then:
        !MDC.get(RequestLog.ID_KEY)?.startsWith(requestId)

        where:
        requestId << [
                "",
                "##",
                "arequest#",
                "5rreques!id",
                "5".multiply(201),
                "5+a-b!=6"
        ]
    }
}
