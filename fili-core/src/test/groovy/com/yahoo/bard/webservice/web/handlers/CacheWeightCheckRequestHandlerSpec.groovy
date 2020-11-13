// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.druid.client.SuccessCallback
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.WeightEvaluationQuery
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfo
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfoUtils
import com.yahoo.bard.webservice.metadata.QuerySigningService
import com.yahoo.bard.webservice.metadata.SegmentIntervalsHashIdGenerator
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor
import com.yahoo.bard.webservice.web.responseprocessors.WeightCheckResponseProcessor
import com.yahoo.bard.webservice.web.util.QueryWeightUtil
import com.yahoo.bard.webservice.data.cache.TupleDataCache
import com.yahoo.bard.webservice.web.util.QuerySignedCacheService

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter

import spock.lang.Specification

class CacheWeightCheckRequestHandlerSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    DataRequestHandler next
    DruidWebService webService
    QueryWeightUtil queryWeightUtil
    ObjectWriter writer
    TupleDataCache<String, Long, String> dataCache
    QuerySigningService<Long> querySigningService
    QuerySignedCacheService cacheService
    WeightEvaluationQuery weightQuery
    CacheWeightCheckRequestHandler handler

    RequestContext context
    DataApiRequest request
    GroupByQuery groupByQuery
    ResponseProcessor response

    BardQueryInfo bardQueryInfo
    JsonNode json

    def setup() {
        next = Mock(DataRequestHandler)
        webService = Mock(DruidWebService)
        queryWeightUtil = Mock(QueryWeightUtil)
        writer = Mock(ObjectWriter)
        MAPPER.writer() >> writer
        context = Mock(RequestContext)
        request = Mock(DataApiRequest)
        dataCache = Mock(TupleDataCache)
        querySigningService = Mock(SegmentIntervalsHashIdGenerator)
        weightQuery = Mock(WeightEvaluationQuery)
        groupByQuery = Mock(GroupByQuery)
        groupByQuery.getGranularity() >> DAY
        groupByQuery.getInnermostQuery() >> groupByQuery
        queryWeightUtil.makeWeightEvaluationQuery(groupByQuery) >> weightQuery
        response = Mock(WeightCheckResponseProcessor)
        bardQueryInfo = BardQueryInfoUtils.initializeBardQueryInfo()
        json = new JsonNodeFactory().arrayNode()
        cacheService = Mock(QuerySignedCacheService)
        handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                MAPPER,
                cacheService
        )
    }

    def cleanup() {
        BardQueryInfoUtils.resetBardQueryInfo()
    }

    def "Check constructor"() {
        expect:
        handler.next == next
        handler.webService == webService
        handler.queryWeightUtil == queryWeightUtil
        handler.mapper == MAPPER
        handler.querySignedCacheService == cacheService
    }

    def "Request passing quick weight check"() {
        setup:
        1 * queryWeightUtil.skipWeightCheckQuery(groupByQuery) >> true
        1 * next.handleRequest(context, request, groupByQuery, response) >> true

        expect:
        handler.handleRequest(context, request, groupByQuery, response)

        and:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0
    }

    def "Weight check is run and success callback is triggered"() {
        setup:
        final SuccessCallback success = Mock(SuccessCallback)
        writer.writeValueAsString(weightQuery) >> "Weight query"
        CacheWeightCheckRequestHandler handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                MAPPER,
                cacheService
        ) {
            @Override
            public SuccessCallback buildCacheSuccessCallback(
                    final SuccessCallback classicSuccessCallback,
                    final QuerySignedCacheService cacheService,
                    final DruidAggregationQuery<?> groupByQuery,
                    final ResponseProcessor response
            ) {
                return success
            }
        }

        when: "Weight check is successfully run"
        handler.handleRequest(context, request, groupByQuery, response)

        then:
        1 * queryWeightUtil.skipWeightCheckQuery(groupByQuery) >> false
        1 * queryWeightUtil.getQueryWeightThreshold(DAY) >> 5
        1 * webService.postDruidQuery(context, success, null, null, weightQuery)
        0 * next.handleRequest(_)

        and:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 1
    }

    def "Weight check is run and success callback is triggered with json error"() {
        setup:
        final SuccessCallback success = Mock(SuccessCallback)
        writer.writeValueAsString(weightQuery) >> { throw new JsonProcessingException("word") }
        CacheWeightCheckRequestHandler handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                MAPPER,
                cacheService
        ) {
            @Override
            public SuccessCallback buildCacheSuccessCallback(
                    final SuccessCallback classicSuccessCallback,
                    final QuerySignedCacheService cacheService,
                    final DruidAggregationQuery<?> groupByQuery,
                    final ResponseProcessor response
            ) {
                return success
            }
        }

        when:
        handler.handleRequest(context, request, groupByQuery, response)

        then:
        1 * queryWeightUtil.skipWeightCheckQuery(groupByQuery) >> false
        1 * queryWeightUtil.getQueryWeightThreshold(DAY) >> 5
        1 * webService.postDruidQuery(context, success, null, null, weightQuery)
        0 * next.handleRequest(_)

        and:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 1
    }

    def "On cache hit, handler responds to the group by request"() {
        setup:
        RequestContext requestContext = Mock(RequestContext)
        requestContext.isReadCache() >> true

        expect: "The count of fact query cache hit is 0"
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 0

        when: "A groupBy query runs with a valid cache hit"
        boolean requestProcessed = handler.handleRequest(requestContext, request, groupByQuery, response)

        then: "The request is marked as processed"
        requestProcessed
    }

    def "On cache hit, handler responds to the group by request with exception in cache read"() {
        setup:
        RequestContext requestContext = Mock(RequestContext)
        requestContext.isReadCache() >> true
        cacheService.readCache(_) >> { throw new JsonProcessingException(_) }

        expect: "The count of fact query cache hit is 0"
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 0

        when: "A groupBy query runs with a valid cache hit"
        boolean requestProcessed = handler.handleRequest(requestContext, request, groupByQuery, response)

        then: "The request is marked as processed"
        requestProcessed

        and: "The count of fact query cache hit is incremented by 1"
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 0
    }
}
