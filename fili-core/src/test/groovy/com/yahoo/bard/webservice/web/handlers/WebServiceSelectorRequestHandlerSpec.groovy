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

    DruidWebService webService = Mock(DruidWebService)
    DataRequestHandler webServiceNext = Mock(DataRequestHandler)
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
        webService,
        webServiceNext,
        mapper
    )

    def "Test constructor initializes properly"() {
        expect:
        def selector = handler.handlerSelector as DefaultWebServiceHandlerSelector
        selector.webServiceHandler.getWebService() == webService
        selector.webServiceHandler.next == webServiceNext
        handler.writer != null
    }

    @Unroll
    def "Test handle request ui timeout #timeout priority #priority"() {
        setup:
        GroupByQuery expectedQuery = (changeGroupBy ? modifiedGroupByQuery : groupByQuery)
        QueryContext expectedContext = new QueryContext([:]).withTimeout(timeout).withPriority(priority)
        DruidWebService onWebService
        DataRequestHandler nextHandler
        ContainerRequestContext crc = Mock(ContainerRequestContext)
        crc.getHeaders() >> headerMap
        headerMap.add(DataApiRequestTypeIdentifier.CLIENT_HEADER_NAME, DataApiRequestTypeIdentifier.CLIENT_HEADER_VALUE)
        headerMap.add("referer", "http://somewhere")
        onWebService = webService
        nextHandler = webServiceNext
        rc = new RequestContext(crc, true)

        when:
        handler.handleRequest(rc, request, groupByQuery, response)

        then:
        1 * serviceConfig.getPriority() >> priority
        1 * groupByQuery.getContext() >> qc
        (changeContext) * groupByQuery.withContext(expectedContext) >> modifiedGroupByQuery

        1 * onWebService.getTimeout() >> timeout
        1 * onWebService.getServiceConfig() >> serviceConfig
        1 * nextHandler.handleRequest(rc, request, expectedQuery, response)

        where:
        timeout  |  priority  | changeGroupBy   | changeContext
        null     |  null      | false           | 0
        5        |  null      | true            | 1
        null     |  1         | true            | 1
        5        |  1         | true            | 1
        null     |  null      | false           | 0
        5        |  null      | true            | 1
        null     |  1         | true            | 1
        5        |  1         | true            | 1

    }
 }
