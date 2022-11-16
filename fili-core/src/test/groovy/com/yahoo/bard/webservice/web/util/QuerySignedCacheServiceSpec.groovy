// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.cache.MemTupleDataCache
import com.yahoo.bard.webservice.data.cache.TupleDataCache
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfo
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfoUtils
import com.yahoo.bard.webservice.metadata.QuerySigningService
import com.yahoo.bard.webservice.metadata.SegmentIntervalsHashIdGenerator
import com.yahoo.bard.webservice.util.SimplifiedIntervalList
import com.yahoo.bard.webservice.web.RequestUtils
import com.yahoo.bard.webservice.web.handlers.RequestContext
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor
import com.yahoo.bard.webservice.web.responseprocessors.WeightCheckResponseProcessor
import org.joda.time.Interval
import spock.lang.Shared
import spock.lang.Specification

import com.fasterxml.jackson.databind.JsonNode
import spock.lang.Unroll

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.MultivaluedHashMap

import static com.yahoo.bard.webservice.async.ResponseContextUtils.createResponseContext
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.MISSING_INTERVALS_CONTEXT_KEY
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.VOLATILE_INTERVALS_CONTEXT_KEY

class QuerySignedCacheServiceSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.instance

    ResponseProcessor response
    TupleDataCache<String, Long, String> dataCache
    QuerySignedCacheService cacheService
    BardQueryInfo bardQueryInfo
    JsonNode json

    GroupByQuery groupByQuery
    QuerySigningService querySigningService
    ContainerRequestContext containerRequestContext
    RequestContext requestContext

    @Shared SimplifiedIntervalList intervals = new SimplifiedIntervalList()
    @Shared SimplifiedIntervalList nonEmptyIntervals = new SimplifiedIntervalList([new Interval(0, 1)])
    ResponseContext responseContext =  createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): intervals])


    def setup() {
        response = Mock(WeightCheckResponseProcessor)
        cacheService = new QuerySignedCacheService(dataCache, querySigningService, MAPPER)
        bardQueryInfo = BardQueryInfoUtils.initializeBardQueryInfo()
        dataCache = Mock(TupleDataCache)
        json = new JsonNodeFactory().arrayNode()
        groupByQuery = RequestUtils.buildGroupByQuery()
        querySigningService = Mock(SegmentIntervalsHashIdGenerator)
        querySigningService.getSegmentSetId(_) >> Optional.of(1234L)
        containerRequestContext = Mock(ContainerRequestContext)
        containerRequestContext.getHeaders() >> (["Bard-Testing": "###BYPASS###", "ClientId": "UI"] as
                MultivaluedHashMap<String, String>)
        cacheService = new QuerySignedCacheService(dataCache, querySigningService, MAPPER)
        requestContext = new RequestContext(containerRequestContext, true)
        requestContext.isReadCache() >> true
        MAPPER.valueToTree(groupByQuery) >> json
    }

    @Unroll
    def "If #description then is cacheable is #expected"() {
        setup:
        response.getResponseContext() >> context

        expect:
        cacheService.isCacheable(response) == expected

        where:
        expected | description                                            | context
        false    | "missing intervals is non empty"                       | createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): nonEmptyIntervals])
        false    | "volatile intervals is non empty"                      | createResponseContext([(VOLATILE_INTERVALS_CONTEXT_KEY.name): nonEmptyIntervals])
        true     | "missing intervals is present and empty"               | createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): new SimplifiedIntervalList()])
        true     | "volatile intervals is present and empty"              | createResponseContext([(VOLATILE_INTERVALS_CONTEXT_KEY.name): new SimplifiedIntervalList()])
        false    | "missing and volatile intervals are non empty"         | createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): nonEmptyIntervals, (VOLATILE_INTERVALS_CONTEXT_KEY.name): nonEmptyIntervals])
        false    | "missing and volatile intervals are present and empty" | createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): new SimplifiedIntervalList(), (VOLATILE_INTERVALS_CONTEXT_KEY.name): nonEmptyIntervals])
    }

    def "Get key from druid query to check match with cache"() {
        setup:
        String expectedResponse = """{"aggregations":[],"context":{},"dataSource":{"name":"dataSource","type":"table"},"dimensions":[],"granularity":{"period":"P1D","type":"period"},"intervals":[],"postAggregations":[],"queryType":"groupBy"}"""

        expect:
        cacheService.getKey(groupByQuery) == expectedResponse
    }

    def "Cache hit responds to the read request with appropriate cached value"() {
        expect: "The count of fact query cache hit is 0"
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 0

        when: "A query runs with a valid cache hit"
        String cachedValue = cacheService.readCache(requestContext, groupByQuery)

        then: "Check the cache and return valid json"
        1 * dataCache.get(_) >> new MemTupleDataCache.DataEntry<String,String>("key1", 1234L, "value")

        then: "The cahed value is retrieved"
        cachedValue == "value"

        and: "The count of fact query cache hit is incremented by 1"
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 1
    }

    def "Cache miss returns null value to the read request"() {
        expect: "The count of fact query cache hit is 0"
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 0

        when: "A request is sent that has a cache miss"
        String cachedValue = cacheService.readCache(requestContext, groupByQuery)

        then: "The cache is checked for a match and misses"
        1 * dataCache.get(_) >> null

        then: "The cache read returns null"
        cachedValue == null

        and: "The count of fact query cache hit is not incremented"
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 0
    }

    def "Cache miss due to segment invalidation returns null value to the read request"() {
        expect: "The count of fact query cache hit is 0"
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 0

        when: "A request is sent that has a cache miss"
        String cachedValue = cacheService.readCache(requestContext, groupByQuery)

        then: "Check the cache and return a stale entry"
        1 * dataCache.get(_) >> new MemTupleDataCache.DataEntry<String,String>("key1", 5678L, "[]")

        then: "We delegate to the next handler, wrapping in a CacheV2ResponseProcessor"
        cachedValue == null

        and: "The count of fact query cache hit is not incremented"
        bardQueryInfo.queryCounter.get(BardQueryInfo.FACT_QUERY_CACHE_HIT).get() == 0
    }

    def "Cache write completes with good cache key"() {
        setup:
        GroupByQuery groupByQuery = Mock(GroupByQuery)
        QuerySigningService<Long> querySigningService = Mock(QuerySigningService)
        querySigningService.getSegmentSetId(_) >> Optional.of(1234L)
        Integer segmentId = querySigningService.getSegmentSetId(groupByQuery).get()
        String cacheKey = '{"aggregations":null,"dataSource":null,"granularity":null,"intervals":null,"queryType":null}'
        SimplifiedIntervalList intervals = new SimplifiedIntervalList()
        ResponseContext responseContext =  createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): intervals])
        response.getResponseContext() >> responseContext

        when:
        cacheService.writeCache(response, json, groupByQuery)

        then:
        1 * dataCache.set(cacheKey, segmentId, '[]')

    }

    def "Overly long data doesn't cache"() {
        setup: "Save the old max-length-to-cache so we can restore it later"
        String max_druid_response_length_to_cache_key = SYSTEM_CONFIG.getPackageVariableName(
                "druid_max_response_length_to_cache"
        )
        GroupByQuery groupByQuery = Mock(GroupByQuery)
        QuerySigningService<Long> querySigningService = Mock(QuerySigningService)
        querySigningService.getSegmentSetId(_) >> Optional.of(1234L)
        SimplifiedIntervalList intervals = new SimplifiedIntervalList()
        ResponseContext responseContext =  createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): intervals])
        response.getResponseContext() >> responseContext

        and: "A very small max-length-to-cache"
        long smallMaxLength = 1L
        SYSTEM_CONFIG.setProperty(max_druid_response_length_to_cache_key, smallMaxLength.toString())

        and: "A caching service to test"
        cacheService = new QuerySignedCacheService(dataCache, querySigningService, MAPPER)

        expect: "The JSON representation is longer than the small max length"
        MAPPER.writer().writeValueAsString(json).length() > smallMaxLength

        when: "We try to cache a value longer than the small max length"
        cacheService.writeCache(response, json, groupByQuery)

        then: "Set is never called on the cache and the next handler is called"
        0 * dataCache.set(*_)

        cleanup: "Restore the original setting for max-length-to-cache"
        SYSTEM_CONFIG.clearProperty(max_druid_response_length_to_cache_key )
    }
}
