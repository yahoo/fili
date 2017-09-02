// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import com.yahoo.bard.webservice.druid.client.DruidWebService
import spock.lang.Specification

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.MultivaluedHashMap

class DefaultWebServiceHandlerSelectorSpec extends Specification {

    DruidWebService webService = Mock(DruidWebService)
    DataRequestHandler webServiceNext = Mock(DataRequestHandler)


    DefaultWebServiceHandlerSelector selector = new DefaultWebServiceHandlerSelector(
            webService,
            webServiceNext,
    )

    def "Test constructor initializes properly"() {
        expect:
        selector.webServiceHandler.getWebService() == webService
        selector.webServiceHandler.next == webServiceNext
    }

    def "Test select"() {
        setup:
        ContainerRequestContext c = Mock(ContainerRequestContext)
        c.getHeaders() >> new MultivaluedHashMap<>()
        RequestContext rc = new RequestContext(c, true)

        expect:
        selector.select(null, null, rc).getWebService() == webService
        selector.select(null, null, rc).next == webServiceNext
    }
}
