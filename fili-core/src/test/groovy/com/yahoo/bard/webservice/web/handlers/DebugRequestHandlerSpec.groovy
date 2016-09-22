// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.web.DataApiRequest
import com.yahoo.bard.webservice.web.ResponseFormatType
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter

import spock.lang.Specification

class DebugRequestHandlerSpec extends Specification {

    static List<ResponseFormatType> debuggingFormats = [ResponseFormatType.DEBUG]

    DataRequestHandler next = Mock(DataRequestHandler)
    ObjectMapper mapper = Mock(ObjectMapper)
    ObjectWriter writer = Mock(ObjectWriter)
    RequestContext context = Mock(RequestContext)
    DataApiRequest request = Mock(DataApiRequest)
    GroupByQuery groupByQuery = Mock(GroupByQuery)
    ResponseProcessor response = Mock(ResponseProcessor)


    DebugRequestHandler handler = new DebugRequestHandler(
        next,
        mapper
    )
    def setup() {
        mapper.writer() >> writer
    }

    def "Test constructor initializes state"() {
        expect:
        handler.next == next
        handler.mapper == mapper
    }

    def "Test non debug requests flow through"() {
        when:
        handler.handleRequest(context, request, groupByQuery, response)

        then:
        1 * request.getFormat() >> formatType
        1 * next.handleRequest(context, request, groupByQuery, response)

        where:
        formatType << ResponseFormatType.values().grep{ it != ResponseFormatType.DEBUG}
    }

    def "Test debug request calls back with message"() {
        setup:
        HttpErrorCallback ec = Mock(HttpErrorCallback)

        when:
        handler.handleRequest(context, request, groupByQuery, response)

        then:
        1 * response.getErrorCallback(groupByQuery) >> ec
        2 * request.getFormat() >> ResponseFormatType.DEBUG
        0 * next.handleRequest(_, _, _, _)
        1 * ec.dispatch(200, ResponseFormatType.DEBUG.name(), "DEBUG")
    }
}
