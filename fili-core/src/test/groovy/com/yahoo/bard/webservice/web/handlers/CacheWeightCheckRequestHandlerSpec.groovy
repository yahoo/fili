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
import com.yahoo.bard.webservice.web.RequestUtils
import com.yahoo.bard.webservice.data.cache.TupleDataCache
import com.yahoo.bard.webservice.data.cache.MemTupleDataCache
import com.yahoo.bard.webservice.web.util.QuerySignedCacheService

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.MultivaluedHashMap

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
        groupByQuery = Mock(GroupByQuery)
        groupByQuery.getInnermostQuery() >> groupByQuery
        response = Mock(WeightCheckResponseProcessor)
        bardQueryInfo = BardQueryInfoUtils.initializeBardQueryInfo()
        json = new JsonNodeFactory().arrayNode()
        cacheService = new QuerySignedCacheService(dataCache, querySigningService, MAPPER)
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

    def "Request handled without building callback"() {
        setup:
        final SuccessCallback success = Mock(SuccessCallback)
        groupByQuery.getGranularity() >> DAY
        groupByQuery.getInnermostQuery() >> groupByQuery
        WeightEvaluationQuery weightQuery = Mock(WeightEvaluationQuery)
        queryWeightUtil.makeWeightEvaluationQuery(groupByQuery) >> weightQuery
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
                groupByQuery.clone()
                return success
            }
        }

        1 * queryWeightUtil.skipWeightCheckQuery(groupByQuery) >> false
        0 * next.handleRequest(context, request, groupByQuery, response) >> true
        1 * queryWeightUtil.getQueryWeightThreshold(DAY) >> 5
        1 * groupByQuery.clone() >> groupByQuery
        1 * webService.postDruidQuery(context, success, null, null, weightQuery)
        1 * response.getErrorCallback(groupByQuery)
        1 * response.getFailureCallback(groupByQuery)

        expect:
        handler.handleRequest(context, request, groupByQuery, response)

        and:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 1
    }

    def "Request handled without building callback with json error"() {
        setup:
        final SuccessCallback success = Mock(SuccessCallback)
        groupByQuery.getGranularity() >> DAY
        WeightEvaluationQuery weightQuery = Mock(WeightEvaluationQuery)
        queryWeightUtil.makeWeightEvaluationQuery(groupByQuery) >> weightQuery
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
                groupByQuery.clone()
                return success
            }
        }

        1 * queryWeightUtil.skipWeightCheckQuery(groupByQuery) >> false
        0 * next.handleRequest(context, request, groupByQuery, response) >> true
        1 * queryWeightUtil.getQueryWeightThreshold(DAY) >> 5
        1 * groupByQuery.clone() >> groupByQuery
        1 * webService.postDruidQuery(context, success, null, null, weightQuery)
        1 * response.getErrorCallback(groupByQuery)
        1 * response.getFailureCallback(groupByQuery)

        expect:
        handler.handleRequest(context, request, groupByQuery, response)

        and:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 1
    }

    def "On cache hit, handler responds to the group by request"() {
        setup:
        GroupByQuery groupByQuery = RequestUtils.buildGroupByQuery()
        QuerySigningService querySigningService = Mock(SegmentIntervalsHashIdGenerator)
        querySigningService.getSegmentSetId(_) >> Optional.of(1234L)
        ContainerRequestContext containerRequestContext = Mock(ContainerRequestContext)
        containerRequestContext.getHeaders() >> (["Bard-Testing": "###BYPASS###", "ClientId": "UI"] as
                MultivaluedHashMap<String, String>)
        RequestContext requestContext = new RequestContext(containerRequestContext, true)
        requestContext.isReadCache() >> true
        MAPPER.valueToTree(groupByQuery) >> json
        cacheService = new QuerySignedCacheService(dataCache, querySigningService, MAPPER)
        CacheWeightCheckRequestHandler handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                MAPPER,
                cacheService
        )

        expect: "The count of fact query cache hit is 0"
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 0

        when: "A groupBy query runs with a valid cache hit"
        boolean requestProcessed = handler.handleRequest(requestContext, request, groupByQuery, response)

        then: "Check the cache and return valid json"
        1 * dataCache.get(_) >> new MemTupleDataCache.DataEntry<String>("key1", 1234L, "[]")

        and: "The request is marked as processed"
        requestProcessed

        and: "The count of fact query cache hit is incremented by 1"
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 1
    }

    def "On cache hit, handler responds to the group by request with exception in cache read"() {
        setup:
        GroupByQuery groupByQuery = RequestUtils.buildGroupByQuery()
        QuerySigningService querySigningService = Mock(SegmentIntervalsHashIdGenerator)
        querySigningService.getSegmentSetId(_) >> Optional.of(1234L)
        ContainerRequestContext containerRequestContext = Mock(ContainerRequestContext)
        containerRequestContext.getHeaders() >> (["Bard-Testing": "###BYPASS###", "ClientId": "UI"] as
                MultivaluedHashMap<String, String>)
        RequestContext requestContext = new RequestContext(containerRequestContext, true)
        requestContext.isReadCache() >> true
        cacheService = new QuerySignedCacheService(dataCache, querySigningService, MAPPER)
        CacheWeightCheckRequestHandler handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                MAPPER,
                cacheService
        )

        expect: "The count of fact query cache hit is 0"
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 0

        when: "A groupBy query runs with a valid cache hit"
        boolean requestProcessed = handler.handleRequest(requestContext, request, groupByQuery, response)

        then: "Check the cache and return valid json"
        1 * dataCache.get(_) >> new MemTupleDataCache.DataEntry<String>("key1", 1234L, "[]")

        and: "The request is marked as processed"
        requestProcessed

        and: "The count of fact query cache hit is incremented by 1"
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 1
    }
}
