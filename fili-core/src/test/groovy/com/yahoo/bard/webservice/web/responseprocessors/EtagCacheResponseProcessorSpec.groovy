// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR
import static javax.ws.rs.core.Response.Status.NOT_MODIFIED
import static javax.ws.rs.core.Response.Status.OK

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.cache.TupleDataCache
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.web.ErrorMessageFormat

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification

class EtagCacheResponseProcessorSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()
    private static final int INTERNAL_SERVER_ERROR_STATUS_CODE = INTERNAL_SERVER_ERROR.getStatusCode()
    private static final String CACHE_KEY = "cacheKey"

    HttpErrorCallback httpErrorCallback
    DruidAggregationQuery druidAggregationQuery
    ResponseProcessor next
    TupleDataCache<String, String, String> dataCache
    EtagCacheResponseProcessor etagCacheResponseProcessor

    def setup() {
        druidAggregationQuery = Mock(DruidAggregationQuery)
        httpErrorCallback = Mock(HttpErrorCallback)
        next = Mock(ResponseProcessor)
        next.getErrorCallback(druidAggregationQuery) >> httpErrorCallback
        dataCache = Mock(TupleDataCache)
        etagCacheResponseProcessor = new EtagCacheResponseProcessor(
                next,
                CACHE_KEY,
                dataCache,
                MAPPER
        )
    }

    def "processResponse reports error when status code is missing"() {
        given:
        JsonNode json = Mock(JsonNode)
        json.has(DruidJsonResponseContentKeys.STATUS_CODE.getName()) >> false

        when:
        etagCacheResponseProcessor.processResponse(json, druidAggregationQuery, Mock(LoggingContext))

        then:
        1 * httpErrorCallback.dispatch(
                INTERNAL_SERVER_ERROR_STATUS_CODE,
                ErrorMessageFormat.INTERNAL_SERVER_ERROR_REASON_PHRASE.format(),
                ErrorMessageFormat.STATUS_CODE_MISSING_FROM_RESPONSE.format()
        )
    }

    def "processResponse reads response from cache when status code is NOT_MODIFIED"() {
        given:
        JsonNode json = MAPPER.readTree(
                """{"${DruidJsonResponseContentKeys.STATUS_CODE.name}": $NOT_MODIFIED.statusCode}"""
        )
        dataCache.getDataValue(CACHE_KEY) >> '[{"k1":"v1"}]'

        when:
        etagCacheResponseProcessor.processResponse(json, druidAggregationQuery, Mock(LoggingContext))

        then:
        json.get(DruidJsonResponseContentKeys.RESPONSE.getName()) == MAPPER.readTree('[{"k1":"v1"}]')
        json.get(DruidJsonResponseContentKeys.CACHED_RESPONSE.getName()).asBoolean()
    }

    def "When status code is OK but etag is missing, processResponse moves on without caching"() {
        given:
        JsonNode json = MAPPER.readTree(
                """{"${DruidJsonResponseContentKeys.STATUS_CODE.name}": $OK.statusCode}"""
        )

        when:
        etagCacheResponseProcessor.processResponse(json, druidAggregationQuery, Mock(LoggingContext))

        then:
        1 * next.processResponse(null, druidAggregationQuery, _ as LoggingContext)
        !json.get(DruidJsonResponseContentKeys.CACHED_RESPONSE.getName()).asBoolean()
    }

    def "processResponse caches response, including etag, on OK response"() {
        given:
        JsonNode json = MAPPER.readTree(
                """
                        {
                            "${DruidJsonResponseContentKeys.ETAG.name}": "someEtag",
                            "${DruidJsonResponseContentKeys.STATUS_CODE.name}": ${OK.statusCode}
                        }
                """,
        )

        when:
        etagCacheResponseProcessor.processResponse(json, druidAggregationQuery, Mock(LoggingContext))

        then:
        1 * dataCache.set(CACHE_KEY, "someEtag", 'null')
        !json.get(DruidJsonResponseContentKeys.CACHED_RESPONSE.getName()).asBoolean()
    }
}
