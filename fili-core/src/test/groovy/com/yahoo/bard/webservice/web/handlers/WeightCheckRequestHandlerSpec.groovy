// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.client.SuccessCallback
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.WeightEvaluationQuery
import com.yahoo.bard.webservice.web.DataApiRequest
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor
import com.yahoo.bard.webservice.web.responseprocessors.WeightCheckResponseProcessor
import com.yahoo.bard.webservice.web.util.QueryWeightUtil

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter

import spock.lang.Specification

class WeightCheckRequestHandlerSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    DataRequestHandler next
    DruidWebService webService
    QueryWeightUtil queryWeightUtil
    ObjectMapper mapper
    ObjectWriter writer

    RequestContext context
    DataApiRequest request
    GroupByQuery groupByQuery
    ResponseProcessor response

    def setup() {
        next = Mock(DataRequestHandler)
        webService = Mock(DruidWebService)
        queryWeightUtil = Mock(QueryWeightUtil)
        mapper = Mock(ObjectMapper)
        writer = Mock(ObjectWriter)
        mapper.writer() >> writer
        context = Mock(RequestContext)
        request = Mock(DataApiRequest)
        groupByQuery = Mock(GroupByQuery)
        groupByQuery.getInnermostQuery() >> groupByQuery
        response = Mock(WeightCheckResponseProcessor)
    }

    def "Test constructor"() {
        setup:
        WeightCheckRequestHandler handler = new WeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                mapper
        )

        expect:
        handler.next == next
        handler.webService == webService
        handler.queryWeightUtil == queryWeightUtil
        handler.mapper == mapper
    }

    def "Test handle request with request passing quick weight check"() {
        setup:
        WeightCheckRequestHandler handler = new WeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                mapper
        )
        1 * queryWeightUtil.skipWeightCheckQuery(groupByQuery) >> true
        1 * next.handleRequest(context, request, groupByQuery, response) >> true

        expect:
        handler.handleRequest(context, request, groupByQuery, response)
    }

    def "Test handleRequest without building callback"() {
        setup:
        final SuccessCallback success = Mock(SuccessCallback)
        groupByQuery.getGranularity() >> DAY
        groupByQuery.getInnermostQuery() >> groupByQuery
        WeightEvaluationQuery weightQuery = Mock(WeightEvaluationQuery)
        queryWeightUtil.makeWeightEvaluationQuery(groupByQuery) >> weightQuery
        1 * groupByQuery.clone() >> groupByQuery
        WeightCheckRequestHandler handler = new WeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                mapper
        ) {
            @Override
            public SuccessCallback buildSuccessCallback(
                    final RequestContext context,
                    final DataApiRequest request,
                    final DruidAggregationQuery<?> groupByQuery,
                    final ResponseProcessor response,
                    final long queryRowLimit
            ) {
                assert queryRowLimit == 5
                groupByQuery.clone()
                return success
            }
        }

        1 * queryWeightUtil.skipWeightCheckQuery(groupByQuery) >> false
        0 * next.handleRequest(context, request, groupByQuery, response) >> true
        1 * queryWeightUtil.getQueryWeightThreshold(DAY) >> 5
        1 * writer.writeValueAsString(weightQuery) >> "Weight query"
        1 * webService.postDruidQuery(context, success, null, null, weightQuery)
        1 * response.getErrorCallback(groupByQuery)
        1 * response.getFailureCallback(groupByQuery)

        expect:
        handler.handleRequest(context, request, groupByQuery, response)
    }

    def "Test handleRequest without building callback with json error"() {
        setup:
        final SuccessCallback success = Mock(SuccessCallback)
        groupByQuery.getGranularity() >> DAY
        WeightEvaluationQuery weightQuery = Mock(WeightEvaluationQuery)
        queryWeightUtil.makeWeightEvaluationQuery(groupByQuery) >> weightQuery
        1 * groupByQuery.clone() >> groupByQuery
        WeightCheckRequestHandler handler = new WeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                mapper
        ) {
            public SuccessCallback buildSuccessCallback(
                    final RequestContext context,
                    final DataApiRequest request,
                    final DruidAggregationQuery<?> groupByQuery,
                    final ResponseProcessor response,
                    final long queryRowLimit
            ) {
                assert queryRowLimit == 5
                groupByQuery.clone()
                return success
            }
        }

        1 * queryWeightUtil.skipWeightCheckQuery(groupByQuery) >> false
        0 * next.handleRequest(context, request, groupByQuery, response) >> true
        1 * queryWeightUtil.getQueryWeightThreshold(DAY) >> 5
        1 * writer.writeValueAsString(weightQuery) >> { throw new JsonProcessingException("word") }
        1 * webService.postDruidQuery(context, success, null, null, weightQuery)
        1 * response.getErrorCallback(groupByQuery)
        1 * response.getFailureCallback(groupByQuery)

        expect:
        handler.handleRequest(context, request, groupByQuery, response)
    }


    def "Test build and invoke success callback passes"() {
        setup:
        WeightCheckRequestHandler handler = new WeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                MAPPER
        )
        int expectedCount = 60
        int limit = 100
        SuccessCallback success = handler.buildSuccessCallback(context, request, groupByQuery, response, limit)
        String weightResponse = """
        [ {
            "version" : "v1",
            "timestamp" : "2012-01-01T00:00:00.000Z",
            "event" : {
                "count" : "$expectedCount"
            }
        } ]
        """
        JsonParser parser = new JsonFactory().createParser(weightResponse)
        JsonNode jsonResult = MAPPER.readTree(parser)
        1 * next.handleRequest(context, request, groupByQuery, response)

        when:
        success.invoke(jsonResult)

        then:
        1 == 1
    }

    def "Test build and invoke success callback count too high"() {
        setup:
        WeightCheckRequestHandler handler = new WeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                mapper
        )
        int expectedCount = 200
        int limit = 100
        SuccessCallback success = handler.buildSuccessCallback(context, request, groupByQuery, response, limit)
        String weightResponse = """
        [ {
            "version" : "v1",
            "timestamp" : "2012-01-01T00:00:00.000Z",
            "event" : {
                "count" : "$expectedCount"
            }
        } ]
        """
        JsonParser parser = new JsonFactory().createParser(weightResponse)
        JsonNode jsonResult = new ObjectMapper().readTree(parser)
        0 * next.handleRequest(context, request, groupByQuery, response)
        HttpErrorCallback ec = Mock(HttpErrorCallback)
        1 * response.getErrorCallback(groupByQuery) >> ec
        1 * ec.dispatch(507, _, _)
        when:
        success.invoke(jsonResult)

        then:
        1 == 1
    }

    def "Test build and invoke success callback invalid json"() {
        setup:
        WeightCheckRequestHandler handler = new WeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                mapper
        )
        int expectedCount = 200
        int limit = 100
        SuccessCallback success = handler.buildSuccessCallback(context, request, groupByQuery, response, limit)
        String weightResponse = """
        [ {
            "version" : "v1",
            "timestamp" : "2012-01-01T00:00:00.000Z",
            "event" : {
                "count2" : "$expectedCount"
            }
        } ]
        """
        JsonParser parser = new JsonFactory().createParser(weightResponse)
        JsonNode jsonResult = new ObjectMapper().readTree(parser)
        0 * next.handleRequest(context, request, groupByQuery, response)
        FailureCallback fc = Mock(FailureCallback)
        1 * response.getFailureCallback(groupByQuery) >> fc
        1 * fc.dispatch(_)

        when:
        success.invoke(jsonResult)

        then:
        1 == 1
    }
}
