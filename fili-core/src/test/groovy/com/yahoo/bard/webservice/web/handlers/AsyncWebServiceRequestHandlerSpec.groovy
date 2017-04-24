// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.druid.client.SuccessCallback
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.web.DataApiRequest
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter

import spock.lang.Specification

import java.util.concurrent.Future

class AsyncWebServiceRequestHandlerSpec extends Specification {

    def "Test handle request invokes asynch call"() {
        setup:
        DruidWebService dws = Mock(DruidWebService)
        RequestContext rc = Mock(RequestContext)
        DataApiRequest request = Mock(DataApiRequest)
        GroupByQuery groupByQuery = Mock(GroupByQuery)
        ResponseProcessor response = Mock(ResponseProcessor)
        JsonNode rootNode = Mock(JsonNode)

        ObjectMapper mapper = Mock(ObjectMapper)
        ObjectWriter writer = Mock(ObjectWriter)
        mapper.writer() >> writer
        AsyncWebServiceRequestHandler handler = new AsyncWebServiceRequestHandler(dws, mapper)

        SuccessCallback sc = null
        boolean success
        when:
        success = handler.handleRequest(rc, request, groupByQuery, response)

        then:
        success
        1 * response.getErrorCallback(groupByQuery)
        1 * response.getFailureCallback(groupByQuery)
        1 * dws.postDruidQuery(rc, _, null, null, groupByQuery) >> { a0, a1, a2, a3, a4 ->
            // Save the success callback
            sc = a1
            return Mock(Future)
        }

        when:
        sc.invoke(rootNode)

        then:
        1 * response.processResponse(rootNode, groupByQuery, _)
    }
}
