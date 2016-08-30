// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors

import static com.yahoo.bard.webservice.async.ResponseContextUtils.createResponseContext
import static com.yahoo.bard.webservice.util.SimplifiedIntervalList.NO_INTERVALS
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.MISSING_INTERVALS_CONTEXT_KEY
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.VOLATILE_INTERVALS_CONTEXT_KEY

import com.yahoo.bard.webservice.data.cache.DataCache
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper
import com.yahoo.bard.webservice.druid.client.FailureCallback
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.util.SimplifiedIntervalList
import com.yahoo.bard.webservice.web.DataApiRequest

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class CachingResponseProcessorSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    ResponseProcessor next = Mock(ResponseProcessor)
    String cacheKey = "SampleKey"
    DataCache<String> dataCache = Mock(DataCache)

    DataApiRequest apiRequest = Mock(DataApiRequest)
    GroupByQuery groupByQuery = Mock(GroupByQuery)
    List<ResultSetMapper> mappers = new ArrayList<ResultSetMapper>()
    @Shared SimplifiedIntervalList intervals = NO_INTERVALS
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
        true     | createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name) : NO_INTERVALS])
        true     | createResponseContext([(VOLATILE_INTERVALS_CONTEXT_KEY.name) : NO_INTERVALS])
        false    | createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): nonEmptyIntervals, (VOLATILE_INTERVALS_CONTEXT_KEY.name): nonEmptyIntervals])
        false    | createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name): NO_INTERVALS, (VOLATILE_INTERVALS_CONTEXT_KEY.name): nonEmptyIntervals])
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
        0 * dataCache.set(_, _)
    }

    def "Partial data doesn't cache and then continues"() {
        setup:
        ResponseContext responseContext = createResponseContext([(MISSING_INTERVALS_CONTEXT_KEY.name) : nonEmptyIntervals])

        when:
        crp.processResponse(json, groupByQuery, null)

        then:
        2 * next.getResponseContext() >> responseContext
        1 * next.processResponse(json, groupByQuery, null)
        0 * dataCache.set(cacheKey, _)
    }

    def "Volatile data doesn't cache and then continues"() {
        setup:
        ResponseContext responseContext = createResponseContext([(VOLATILE_INTERVALS_CONTEXT_KEY.name) : nonEmptyIntervals])

        when:
        crp.processResponse(json, groupByQuery, null)

        then:
        2 * next.getResponseContext() >> responseContext
        1 * next.processResponse(json, groupByQuery, null)
        0 * dataCache.set(cacheKey, '[]')
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
