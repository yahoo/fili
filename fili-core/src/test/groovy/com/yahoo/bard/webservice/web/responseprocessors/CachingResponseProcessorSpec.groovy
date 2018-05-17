// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors

import static com.yahoo.bard.webservice.async.ResponseContextUtils.createResponseContext
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.MISSING_INTERVALS_CONTEXT_KEY
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.VOLATILE_INTERVALS_CONTEXT_KEY

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.cache.DataCache
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper
import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.util.SimplifiedIntervalList
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.node.JsonNodeFactory

import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class CachingResponseProcessorSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.instance

    ResponseProcessor next = Mock(ResponseProcessor)
    String cacheKey = "SampleKey"
    DataCache<String> dataCache = Mock(DataCache)

    DataApiRequest apiRequest = Mock(DataApiRequest)
    GroupByQuery groupByQuery = Mock(GroupByQuery)
    List<ResultSetMapper> mappers = new ArrayList<ResultSetMapper>()
    @Shared SimplifiedIntervalList intervals = new SimplifiedIntervalList()
    @Shared SimplifiedIntervalList nonEmptyIntervals = new SimplifiedIntervalList([new Interval(0, 1)])

    ResponseContext responseContext = createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name) : intervals])

    JsonNodeFactory jsonFactory = new JsonNodeFactory()
    JsonNode json = jsonFactory.arrayNode()

    ObjectWriter writer = Mock(ObjectWriter)

    CachingResponseProcessor crp = new CachingResponseProcessor(next, cacheKey, dataCache, MAPPER)

    def "Test Constructor"() {
        setup:
        1 * next.getResponseContext() >> responseContext

        expect:
        crp.next == next
        crp.cacheKey == cacheKey
        crp.dataCache == dataCache
        crp.getResponseContext() == responseContext
    }

    @Unroll
    def "With responseContext: #context isCacheable returns #expected"() {
        setup:
        2 * next.getResponseContext() >> context

        expect:
        crp.cacheable == expected

        where:
        expected | context
        true     | [:]
        false    | createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name) : nonEmptyIntervals])
        false    | createResponseContext([(VOLATILE_INTERVALS_CONTEXT_KEY.name) : nonEmptyIntervals])
        true     | createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name) : new SimplifiedIntervalList()])
        true     | createResponseContext([(VOLATILE_INTERVALS_CONTEXT_KEY.name) : new SimplifiedIntervalList()])
        false    | createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): nonEmptyIntervals, (VOLATILE_INTERVALS_CONTEXT_KEY.name): nonEmptyIntervals])
        false    | createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): new SimplifiedIntervalList(), (VOLATILE_INTERVALS_CONTEXT_KEY.name): nonEmptyIntervals])
    }

    def "Process response stored and continues without partial and good cache key"() {
        when:
        crp.processResponse(json, groupByQuery, null)

        then:
        1 * next.processResponse(json, groupByQuery, null)
        1 * dataCache.set(cacheKey, '[]')
        next.getResponseContext() >> responseContext

    }

    def "After error saving to cache, process response continues"() {
        when:
        crp.processResponse(json, groupByQuery, null)

        then:
        2 * next.getResponseContext() >> responseContext
        1 * next.processResponse(json, groupByQuery, null)
        1 * dataCache.set(cacheKey, '[]') >> { throw new IllegalStateException() }
    }

    def "After json serialization error of the cache value, process response continues"() {
        setup:
        ObjectMapper localMapper = Mock(ObjectMapper)
        ObjectWriter localWriter = Mock(ObjectWriter)
        localWriter.writeValueAsString(_) >> { throw new IllegalStateException() }
        localMapper.writer() >> localWriter

        CachingResponseProcessor localCrp = new CachingResponseProcessor(next, cacheKey, dataCache, localMapper)

        when:
        localCrp.processResponse(json, groupByQuery, null)

        then:
        2 * next.getResponseContext() >> responseContext
        1 * next.processResponse(json, groupByQuery, null)
        0 * dataCache.set(*_)
    }

    def "Partial data doesn't cache and then continues"() {
        setup:
        ResponseContext responseContext = createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name) : nonEmptyIntervals])

        when:
        crp.processResponse(json, groupByQuery, null)

        then:
        2 * next.getResponseContext() >> responseContext
        1 * next.processResponse(json, groupByQuery, null)
        0 * dataCache.set(*_)
    }

    def "Volatile data doesn't cache and then continues"() {
        setup:
        ResponseContext responseContext = createResponseContext([(VOLATILE_INTERVALS_CONTEXT_KEY.name) : nonEmptyIntervals])

        when:
        crp.processResponse(json, groupByQuery, null)

        then:
        2 * next.getResponseContext() >> responseContext
        1 * next.processResponse(json, groupByQuery, null)
        0 * dataCache.set(*_)
    }

    def "Overly long data doesn't cache and then continues"() {
        setup: "Save the old max-length-to-cache so we can restore it later"
        String max_druid_response_length_to_cache_key = SYSTEM_CONFIG.getPackageVariableName(
                "druid_max_response_length_to_cache"
        )
        long oldMaxLength = SYSTEM_CONFIG.getLongProperty(max_druid_response_length_to_cache_key)

        and: "A very small max-length-to-cache"
        long smallMaxLength = 1L
        SYSTEM_CONFIG.resetProperty(max_druid_response_length_to_cache_key, smallMaxLength.toString())

        and: "A workable response context"
        next.getResponseContext() >> responseContext

        and: "A caching response processor to test"
        crp = new CachingResponseProcessor(next, cacheKey, dataCache, MAPPER)

        expect: "The JSON representation is longer than the small max length"
        MAPPER.writer().writeValueAsString(json).length() > smallMaxLength

        when: "We try to cache a value longer than the small max length"
        crp.processResponse(json, groupByQuery, null)

        then: "Set is never called on the cache and the next handler is called"
        1 * next.processResponse(json, groupByQuery, null)
        0 * dataCache.set(*_)

        cleanup: "Restore the original setting for max-length-to-cache"
        SYSTEM_CONFIG.resetProperty(max_druid_response_length_to_cache_key, oldMaxLength.toString())
    }

    def "Test proxy calls"() {
        setup:
        HttpErrorCallback hec = Mock(HttpErrorCallback)
        FailureCallback fc = Mock(FailureCallback)

        when:
        crp.getErrorCallback(groupByQuery)
        crp.getFailureCallback(groupByQuery)

        then:
        1 * next.getErrorCallback(groupByQuery) >> hec
        1 * next.getFailureCallback(groupByQuery) >> fc
    }
}
