// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.druid.client.DruidServiceConfig
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.QueryContext
import com.yahoo.bard.webservice.web.DataApiRequest
import com.yahoo.bard.webservice.web.DataApiRequestTypeIdentifier
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.MultivaluedMap

class WebServiceSelectorRequestHandlerSpec extends Specification {

    DruidWebService uiWebService = Mock(DruidWebService)
    DruidWebService nonUiWebService = Mock(DruidWebService)
    DataRequestHandler uiWebServiceNext = Mock(DataRequestHandler)
    DataRequestHandler nonUiWebServiceNext = Mock(DataRequestHandler)
    ObjectMapper mapper = new ObjectMappersSuite().getMapper()
    GroupByQuery groupByQuery = Mock(GroupByQuery)
    GroupByQuery modifiedGroupByQuery = Mock(GroupByQuery)
    RequestContext rc
    ResponseProcessor response = Mock(ResponseProcessor)
    MultivaluedMap<String, String> headerMap = new MultivaluedHashMap<>()
    DataApiRequest request = Mock(DataApiRequest)
    QueryContext qc = new QueryContext([:])
    DruidServiceConfig serviceConfig = Mock(DruidServiceConfig)

    WebServiceSelectorRequestHandler handler = new WebServiceSelectorRequestHandler(
        uiWebService,
        nonUiWebService,
        uiWebServiceNext,
        nonUiWebServiceNext,
        mapper
    )

    def "Test constructor initializes properly"() {
        expect:
        def selector = handler.handlerSelector as DefaultWebServiceHandlerSelector
        selector.uiWebServiceHandler.getWebService() == uiWebService
        selector.nonUiWebServiceHandler.getWebService() == nonUiWebService
        selector.uiWebServiceHandler.next == uiWebServiceNext
        selector.nonUiWebServiceHandler.next == nonUiWebServiceNext
        handler.writer != null
    }

    @Unroll
    def "Test handle request ui timeout #timeout priority #priority"() {
        setup:
        GroupByQuery expectedQuery = (changeGroupBy ? modifiedGroupByQuery : groupByQuery)
        QueryContext expectedContext = new QueryContext([:]).withTimeout(timeout).withPriority(priority)
        DruidWebService onWebService
        DruidWebService offWebService
        DataRequestHandler nextHandler
        ContainerRequestContext crc = Mock(ContainerRequestContext)
        crc.getHeaders() >> headerMap
        if (useUI) {
            headerMap.add(DataApiRequestTypeIdentifier.CLIENT_HEADER_NAME, DataApiRequestTypeIdentifier.CLIENT_HEADER_VALUE)
            headerMap.add("referer", "http://somewhere")
            onWebService = uiWebService
            offWebService = nonUiWebService
            nextHandler = uiWebServiceNext
            rc = new RequestContext(crc, true)
        } else {
            onWebService = nonUiWebService
            offWebService = uiWebService
            nextHandler = nonUiWebServiceNext
            rc = new RequestContext(crc, true)
        }

        when:
        handler.handleRequest(rc, request, groupByQuery, response)

        then:
        1 * serviceConfig.getPriority() >> priority
        1 * groupByQuery.getContext() >> qc
        (changeContext) * groupByQuery.withContext(expectedContext) >> modifiedGroupByQuery

        1 * onWebService.getTimeout() >> timeout
        1 * onWebService.getServiceConfig() >> serviceConfig
        1 * nextHandler.handleRequest(rc, request, expectedQuery, response)
        0 * offWebService.getTimeout()
        0 * offWebService.getServiceConfig()

        where:
        timeout  |  priority  | changeGroupBy   | changeContext  | useUI
        null     |  null      | false           | 0              | true
        5        |  null      | true            | 1              | true
        null     |  1         | true            | 1              | true
        5        |  1         | true            | 1              | true
        null     |  null      | false           | 0              | false
        5        |  null      | true            | 1              | false
        null     |  1         | true            | 1              | false
        5        |  1         | true            | 1              | false

    }
 }
