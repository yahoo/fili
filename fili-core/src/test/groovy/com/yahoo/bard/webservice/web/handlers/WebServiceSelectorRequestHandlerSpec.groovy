// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.client.DruidServiceConfig
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.QueryContext
import com.yahoo.bard.webservice.logging.RequestLog
import com.yahoo.bard.webservice.logging.TimeRemainingFunction
import com.yahoo.bard.webservice.web.DataApiRequestTypeIdentifier
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest
import com.yahoo.bard.webservice.web.filters.BardLoggingFilter
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.TimeUnit

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

    def "Test time function counts down based on total timing"() {
        setup:

        WebServiceSelectorRequestHandler handler = new WebServiceSelectorRequestHandler(
                webService,
                webServiceNext,
                mapper,
                TimeRemainingFunction.INSTANCE
        )

        RequestLog.startTiming(BardLoggingFilter.TOTAL_TIMER)
        DataSource dataSource = Mock(DataSource)
        GroupByQuery groupByQuery1 = new GroupByQuery(
                dataSource,
                DefaultTimeGrain.DAY,
                Collections.emptyList(),
                null,
                null,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                null
        )
        Integer timeout = 20000
        Integer timeSoFar = 0
        QueryContext latestActualContext = null

        and: "Stubbing"
        DruidWebService onWebService
        DataRequestHandler nextHandler = webServiceNext
        ContainerRequestContext crc = Mock(ContainerRequestContext)
        crc.getHeaders() >> headerMap
        headerMap.add(DataApiRequestTypeIdentifier.CLIENT_HEADER_NAME, DataApiRequestTypeIdentifier.CLIENT_HEADER_VALUE)
        headerMap.add("referer", "http://somewhere")

        onWebService = webService
        onWebService.getTimeout() >> timeout
        onWebService.getServiceConfig() >> serviceConfig

        nextHandler
        rc = new RequestContext(crc, true)

        when:
        Thread.sleep(1000)
        handler.handleRequest(rc, request, groupByQuery1, response)


        then:
        1 * nextHandler.handleRequest(rc, request, _, response) >> {
            arguments ->
                timeSoFar = (int) TimeUnit.NANOSECONDS.toMillis(RequestLog.fetchTiming(BardLoggingFilter.TOTAL_TIMER).getActiveDuration())
                latestActualContext = ((GroupByQuery) arguments[2]).getContext()
                return true
        }
        timeSoFar > 1000
        timeSoFar < 19000
        Math.abs(timeout - timeSoFar - latestActualContext.getTimeout() ) < 100

        when:
        Thread.sleep(2000)
        handler.handleRequest(rc, request, groupByQuery1, response)


        then:
        1 * nextHandler.handleRequest(rc, request, _, response) >> {
            arguments ->
                timeSoFar = (int) TimeUnit.NANOSECONDS.toMillis(RequestLog.fetchTiming(BardLoggingFilter.TOTAL_TIMER).getActiveDuration())
                latestActualContext = ((GroupByQuery) arguments[2]).getContext()
                return true
        }
        timeSoFar > 3000
        timeSoFar < 17000
        Math.abs(timeout - timeSoFar - latestActualContext.getTimeout() ) < 100

        cleanup:
        RequestLog.stopTiming(BardLoggingFilter.TOTAL_TIMER)
    }
 }
