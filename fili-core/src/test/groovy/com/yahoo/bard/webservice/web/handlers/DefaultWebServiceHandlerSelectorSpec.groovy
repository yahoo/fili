// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import com.yahoo.bard.webservice.druid.client.DruidWebService
import spock.lang.Specification

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.MultivaluedMap

class DefaultWebServiceHandlerSelectorSpec extends Specification {

    DruidWebService uiWebService = Mock(DruidWebService)
    DruidWebService nonUiWebService = Mock(DruidWebService)
    DataRequestHandler uiWebServiceNext = Mock(DataRequestHandler)
    DataRequestHandler nonUiWebServiceNext = Mock(DataRequestHandler)


    DefaultWebServiceHandlerSelector selector = new DefaultWebServiceHandlerSelector(
            uiWebService,
            nonUiWebService,
            uiWebServiceNext,
            nonUiWebServiceNext
    )

    def "Test constructor initializes properly"() {
        expect:
        selector.uiWebServiceHandler.getWebService() == uiWebService
        selector.nonUiWebServiceHandler.getWebService() == nonUiWebService
        selector.uiWebServiceHandler.next == uiWebServiceNext
        selector.nonUiWebServiceHandler.next == nonUiWebServiceNext
    }

    def "Test select"() {
        setup:
        ContainerRequestContext c = Mock(ContainerRequestContext)
        c.getHeaders() >> new MultivaluedHashMap<>()
        RequestContext rc = new RequestContext(c, true)

        expect:
        selector.select(null, null, rc).getWebService() == nonUiWebService
        selector.select(null, null, rc).next == nonUiWebServiceNext
    }
}
