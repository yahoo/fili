// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors

import static javax.ws.rs.core.Response.Status.NOT_MODIFIED
import static javax.ws.rs.core.Response.Status.OK

import com.yahoo.bard.webservice.data.cache.TupleDataCache
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.metadata.QuerySigningService
import com.yahoo.bard.webservice.web.ErrorMessageFormat

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import spock.lang.Specification

class EtagCacheResponseProcessorSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))
    private static final int INTERNAL_SERVER_ERROR_STATUS_CODE = 500

    ResponseProcessor next
    TupleDataCache<String, Long, String> dataCache
    QuerySigningService<Long> querySigningService
    EtagCacheResponseProcessor etagCacheResponseProcessor

    HttpErrorCallback httpErrorCallback
    DruidAggregationQuery druidAggregationQuery

    def setup() {
        next = Mock(ResponseProcessor)
        dataCache = Mock(TupleDataCache)
        querySigningService = Mock(QuerySigningService)
        etagCacheResponseProcessor = new EtagCacheResponseProcessor(
                next,
                dataCache,
                querySigningService,
                new ObjectMapper()
        )

        httpErrorCallback = Mock(HttpErrorCallback)
        druidAggregationQuery = Mock(DruidAggregationQuery)
        next.getErrorCallback(druidAggregationQuery) >> httpErrorCallback
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
                String.format(
                        '{"%s": %d}',
                        DruidJsonResponseContentKeys.STATUS_CODE.getName(),
                        NOT_MODIFIED.getStatusCode()
                )
        )
        dataCache.getDataValue(DruidJsonResponseContentKeys.RESPONSE.getName()) >> '[{"k1":"v1"}]'

        when:
        etagCacheResponseProcessor.processResponse(json, druidAggregationQuery, Mock(LoggingContext))

        then:
        json.get(DruidJsonResponseContentKeys.RESPONSE.getName()) == MAPPER.readTree('[{"k1":"v1"}]')
    }

    def "processResponse reports error when status code is OK but etag is missing"() {
        given:
        JsonNode json = MAPPER.readTree(
                String.format(
                        '{"%s": %d}',
                        DruidJsonResponseContentKeys.STATUS_CODE.getName(),
                        OK.getStatusCode()
                )
        )

        when:
        etagCacheResponseProcessor.processResponse(json, druidAggregationQuery, Mock(LoggingContext))

        then:
        then:
        1 * httpErrorCallback.dispatch(
                INTERNAL_SERVER_ERROR_STATUS_CODE,
                ErrorMessageFormat.INTERNAL_SERVER_ERROR_REASON_PHRASE.format(),
                ErrorMessageFormat.ETAG_MISSING_FROM_RESPONSE.format()
        )
    }

    def "processResponse caches response and etag on OK response"() {
        given:
        querySigningService.getSegmentSetId(_ as DruidAggregationQuery) >> Optional.empty()
        JsonNode json = MAPPER.readTree(
                String.format(
                        '''
                        {
                            "%s": "someEtag",
                            "%s": %d
                        }
                        ''',
                        DruidJsonResponseContentKeys.ETAG.getName(),
                        DruidJsonResponseContentKeys.STATUS_CODE.getName(),
                        OK.getStatusCode()
                )
        )

        when:
        etagCacheResponseProcessor.processResponse(json, druidAggregationQuery, Mock(LoggingContext))

        then:
        1 * dataCache.set(DruidJsonResponseContentKeys.RESPONSE.getName(), null, 'null')
        1 * dataCache.set(DruidJsonResponseContentKeys.ETAG.getName(), null, '"someEtag"')
    }
}
