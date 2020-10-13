// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import static com.yahoo.bard.webservice.async.ResponseContextUtils.createResponseContext
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.MISSING_INTERVALS_CONTEXT_KEY

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
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
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext
import com.yahoo.bard.webservice.web.responseprocessors.WeightCheckResponseProcessor
import com.yahoo.bard.webservice.web.util.QueryWeightUtil
import com.yahoo.bard.webservice.web.util.CacheService
import com.yahoo.bard.webservice.web.RequestUtils
import com.yahoo.bard.webservice.util.SimplifiedIntervalList
import com.yahoo.bard.webservice.data.cache.TupleDataCache
import com.yahoo.bard.webservice.data.cache.MemTupleDataCache

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
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
    ObjectMapper mapper
    ObjectWriter writer
    TupleDataCache<String, Long, String> dataCache
    QuerySigningService<Long> querySigningService

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
        mapper = Mock(ObjectMapper)
        writer = Mock(ObjectWriter)
        mapper.writer() >> writer
        context = Mock(RequestContext)
        request = Mock(DataApiRequest)
        dataCache = Mock(TupleDataCache)
        querySigningService = Mock(SegmentIntervalsHashIdGenerator)
        groupByQuery = Mock(GroupByQuery)
        groupByQuery.getInnermostQuery() >> groupByQuery
        response = Mock(WeightCheckResponseProcessor)
        bardQueryInfo = BardQueryInfoUtils.initializeBardQueryInfo()
        json = new JsonNodeFactory().arrayNode()
    }

    def cleanup() {
        BardQueryInfoUtils.resetBardQueryInfo()
    }

    def "Test constructor"() {
        setup:
        CacheWeightCheckRequestHandler handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                mapper,
                dataCache,
                querySigningService
        )

        expect:
        handler.next == next
        handler.webService == webService
        handler.queryWeightUtil == queryWeightUtil
        handler.mapper == mapper
        handler.dataCache == dataCache
        handler.querySigningService == querySigningService
    }

    def "Test handle request with request passing quick weight check"() {
        setup:
        CacheWeightCheckRequestHandler handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                mapper,
                dataCache,
                querySigningService
        )
        1 * queryWeightUtil.skipWeightCheckQuery(groupByQuery) >> true
        1 * next.handleRequest(context, request, groupByQuery, response) >> true

        expect:
        handler.handleRequest(context, request, groupByQuery, response)

        and:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0
    }

    def "Test handleRequest without building callback"() {
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
                dataCache,
                querySigningService
        ) {
            @Override
            public SuccessCallback buildCacheSuccessCallback(
                    final RequestContext context,
                    final DataApiRequest request,
                    final DruidAggregationQuery<?> groupByQuery,
                    final ResponseProcessor response,
                    final long queryRowLimit,
                    final String cacheKey,
                    final CacheService cacheService,
                    final WeightCheckRequestHandler weightCheckRequestHandler,
                    final Boolean writeCache
            ) {
                assert queryRowLimit == 5
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

    def "Test handleRequest without building callback with json error"() {
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
                dataCache,
                querySigningService
        ) {
            @Override
            public SuccessCallback buildCacheSuccessCallback(
                    final RequestContext context,
                    final DataApiRequest request,
                    final DruidAggregationQuery<?> groupByQuery,
                    final ResponseProcessor response,
                    final long queryRowLimit,
                    final String cacheKey,
                    final CacheService cacheService,
                    final WeightCheckRequestHandler weightCheckRequestHandler,
                    final Boolean writeCache
            ) {
                assert queryRowLimit == 5
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

    def "Test handle request on cache hit responds to the group by request"() {
        setup:
        GroupByQuery groupByQuery = RequestUtils.buildGroupByQuery()
        QuerySigningService querySigningService = Mock(SegmentIntervalsHashIdGenerator)
        querySigningService.getSegmentSetId(_) >> Optional.of(1234L)
        ContainerRequestContext containerRequestContext = Mock(ContainerRequestContext)
        containerRequestContext.getHeaders() >> (["Bard-Testing": "###BYPASS###", "ClientId": "UI"] as
                MultivaluedHashMap<String, String>)
        RequestContext requestContext = new RequestContext(containerRequestContext, true)
        CacheWeightCheckRequestHandler handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                MAPPER,
                dataCache,
                querySigningService
        )
        requestContext.isReadCache() >> true

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

    def "Test handle request on cache hit responds to the group by request with exception in cache read"() {
        setup:
        GroupByQuery groupByQuery = RequestUtils.buildGroupByQuery()
        QuerySigningService querySigningService = Mock(SegmentIntervalsHashIdGenerator)
        querySigningService.getSegmentSetId(_) >> Optional.of(1234L)
        ContainerRequestContext containerRequestContext = Mock(ContainerRequestContext)
        containerRequestContext.getHeaders() >> (["Bard-Testing": "###BYPASS###", "ClientId": "UI"] as
                MultivaluedHashMap<String, String>)
        RequestContext requestContext = new RequestContext(containerRequestContext, true)
        CacheWeightCheckRequestHandler handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                MAPPER,
                dataCache,
                querySigningService
        )
        requestContext.isReadCache() >> true

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

    def "Test cache hit with weight check"() {
        setup:
        setup:
        SimplifiedIntervalList intervals = new SimplifiedIntervalList()
        ResponseContext responseContext =  createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): intervals])
        response.getResponseContext() >> responseContext
        GroupByQuery groupByQuery = RequestUtils.buildGroupByQuery()
        QuerySigningService querySigningService = Mock(SegmentIntervalsHashIdGenerator)
        querySigningService.getSegmentSetId(_) >> Optional.of(1234L)
        ContainerRequestContext containerRequestContext = Mock(ContainerRequestContext)
        containerRequestContext.getHeaders() >> (["Bard-Testing": "###BYPASS###", "ClientId": "UI"] as
                MultivaluedHashMap<String, String>)
        RequestContext requestContext = new RequestContext(containerRequestContext, true)
        CacheWeightCheckRequestHandler handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                MAPPER,
                dataCache,
                querySigningService
        )
        requestContext.isReadCache() >> true
        int expectedCount = 60
        int limit = 100
        String cacheKey = "key1"
        CacheService cacheService = new CacheService()
        WeightCheckRequestHandler weightCheckRequestHandler = new WeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                mapper
        )
        SuccessCallback success = handler.buildCacheSuccessCallback(context, request, groupByQuery, response, limit, cacheKey, cacheService, weightCheckRequestHandler, true)
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

        expect:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0

        when:
        success.invoke(jsonResult)

        then:
        1 * next.handleRequest(context, request, groupByQuery, response)

        and:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0
    }

    def "Test cache hit with weight check when exception thrown by cache read"() {
        setup:
        setup:
        SimplifiedIntervalList intervals = new SimplifiedIntervalList()
        ResponseContext responseContext =  createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): intervals])
        response.getResponseContext() >> responseContext
        GroupByQuery groupByQuery = RequestUtils.buildGroupByQuery()
        QuerySigningService querySigningService = Mock(SegmentIntervalsHashIdGenerator)
        querySigningService.getSegmentSetId(_) >> Optional.of(1234L)
        ContainerRequestContext containerRequestContext = Mock(ContainerRequestContext)
        containerRequestContext.getHeaders() >> (["Bard-Testing": "###BYPASS###", "ClientId": "UI"] as
                MultivaluedHashMap<String, String>)
        RequestContext requestContext = new RequestContext(containerRequestContext, true)
        CacheWeightCheckRequestHandler handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                MAPPER,
                dataCache,
                querySigningService
        )
        requestContext.isReadCache() >> true
        int expectedCount = 60
        int limit = 100
        String cacheKey = "key1"
        CacheService cacheService = new CacheService()
        cacheService.readCache() >> {new Exception()}
        WeightCheckRequestHandler weightCheckRequestHandler = new WeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                mapper
        )
        SuccessCallback success = handler.buildCacheSuccessCallback(context, request, groupByQuery, response, limit, cacheKey, cacheService, weightCheckRequestHandler, true)
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

        expect:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0

        when:
        success.invoke(jsonResult)

        then:
        1 * next.handleRequest(context, request, groupByQuery, response)

        and:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0
    }


    def "Test build and invoke cache success callback passes without cache hit"() {
        setup:
        SimplifiedIntervalList intervals = new SimplifiedIntervalList()
        ResponseContext responseContext =  createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): intervals])
        response.getResponseContext() >> responseContext
        CacheWeightCheckRequestHandler handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                MAPPER,
                dataCache,
                querySigningService
        )
        int expectedCount = 60
        int limit = 100
        String cacheKey = "key1"
        CacheService cacheService = new CacheService()
        WeightCheckRequestHandler weightCheckRequestHandler = new WeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                mapper
        )
        SuccessCallback success = handler.buildCacheSuccessCallback(context, request, groupByQuery, response, limit, cacheKey, cacheService, weightCheckRequestHandler, true)
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

        expect:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0

        when:
        success.invoke(jsonResult)

        then:
        1 * next.handleRequest(context, request, groupByQuery, response)

        and:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0
    }

    def "Test build and invoke cache success callback passes without cache hit with exception thrown by cache write"() {
        setup:
        SimplifiedIntervalList intervals = new SimplifiedIntervalList()
        ResponseContext responseContext =  createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): intervals])
        response.getResponseContext() >> responseContext
        CacheWeightCheckRequestHandler handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                MAPPER,
                dataCache,
                querySigningService
        )
        int expectedCount = 60
        int limit = 100
        String cacheKey = "key1"
        CacheService cacheService = new CacheService()
        cacheService.writeCache() >> {new Exception()}
        WeightCheckRequestHandler weightCheckRequestHandler = new WeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                mapper
        )
        SuccessCallback success = handler.buildCacheSuccessCallback(context, request, groupByQuery, response, limit, cacheKey, cacheService, weightCheckRequestHandler, true)
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

        expect:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0

        when:
        success.invoke(jsonResult)

        then:
        1 * next.handleRequest(context, request, groupByQuery, response)

        and:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0
    }

    def "Test build and invoke cache success callback count too high"() {
        setup:
        SimplifiedIntervalList intervals = new SimplifiedIntervalList()
        ResponseContext responseContext =  createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): intervals])
        response.getResponseContext() >> responseContext
        CacheWeightCheckRequestHandler handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                MAPPER,
                dataCache,
                querySigningService
        )
        WeightCheckRequestHandler weightCheckRequestHandler = new WeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                mapper
        )
        int expectedCount = 200
        int limit = 100
        String cacheKey = "key1"
        CacheService cacheService = new CacheService()
        SuccessCallback success = handler.buildCacheSuccessCallback(context, request, groupByQuery, response, limit, cacheKey, cacheService, weightCheckRequestHandler, true)
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
        HttpErrorCallback ec = Mock(HttpErrorCallback)

        expect:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0

        when:
        success.invoke(jsonResult)

        then:
        0 * next.handleRequest(context, request, groupByQuery, response)
        1 * response.getErrorCallback(groupByQuery) >> ec
        1 * ec.dispatch(507, _, _)

        and:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0
    }

    def "Test build and invoke cache success callback invalid json"() {
        setup:
        SimplifiedIntervalList intervals = new SimplifiedIntervalList()
        ResponseContext responseContext =  createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): intervals])
        response.getResponseContext() >> responseContext
        CacheWeightCheckRequestHandler handler = new CacheWeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                MAPPER,
                dataCache,
                querySigningService
        )
        WeightCheckRequestHandler weightCheckRequestHandler = new WeightCheckRequestHandler(
                next,
                webService,
                queryWeightUtil,
                mapper
        )
        int expectedCount = 200
        int limit = 100
        String cacheKey = "key1"
        CacheService cacheService = new CacheService()
        SuccessCallback success = handler.buildCacheSuccessCallback(context, request, groupByQuery, response, limit, cacheKey, cacheService, weightCheckRequestHandler, true)
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
        FailureCallback fc = Mock(FailureCallback)

        expect:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0

        when:
        success.invoke(jsonResult)

        then:
        0 * next.handleRequest(context, request, groupByQuery, response)
        1 * response.getFailureCallback(groupByQuery) >> fc
        1 * fc.dispatch(_)

        and:
        bardQueryInfo.queryCounter.get(BardQueryInfo.WEIGHT_CHECK).get() == 0
    }
}
