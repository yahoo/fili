// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.cache.DataCache
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery
import com.yahoo.bard.webservice.druid.model.query.TopNQuery
import com.yahoo.bard.webservice.web.DataApiRequest
import com.yahoo.bard.webservice.web.RequestUtils
import com.yahoo.bard.webservice.web.responseprocessors.CachingResponseProcessor
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.node.JsonNodeFactory

import spock.lang.Specification

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.MultivaluedHashMap

class CacheRequestHandlerSpec extends Specification {

    CacheRequestHandler handler

    GroupByQuery groupByQuery = RequestUtils.buildGroupByQuery()
    TopNQuery topNQuery = RequestUtils.buildTopNQuery()
    TimeSeriesQuery timeseriesQuery = RequestUtils.buildTimeSeriesQuery()

    DataRequestHandler next = Mock(DataRequestHandler)

    DataCache<String> dataCache = Mock(DataCache)

    DataApiRequest apiRequest = Mock(DataApiRequest)
    ResponseProcessor response = Mock(ResponseProcessor)
    JsonNode json = new JsonNodeFactory().arrayNode()

    ContainerRequestContext containerRequestContext
    RequestContext requestContext

    ObjectMapper mapper = new ObjectMappersSuite().getMapper()

    def setup() {
        handler = new CacheRequestHandler(next, dataCache, mapper)
        containerRequestContext = Mock(ContainerRequestContext)
        containerRequestContext.getHeaders() >> (["Bard-Testing": "###BYPASS###", "ClientId": "UI"] as
                MultivaluedHashMap<String, String>)
        requestContext = new RequestContext(containerRequestContext, true)
    }

    def "Test constructor initializes object"() {
        expect:
        handler.dataCache == dataCache
        handler.next == next
    }

    def "Test handle request on cache hit responds to the group by request"() {
        when: "A groupBy query runs with a valid cache hit"
        boolean requestProcessed = handler.handleRequest(requestContext, apiRequest, groupByQuery, response)

        then: "Check the cache and return valid json"
        1 * dataCache.get(_) >> "[]"

        then: "Process the Json response"
        1 * response.processResponse(json, groupByQuery, _)

        and: "The request is marked as processed"
        requestProcessed
    }

    def "Test handle request on cache hit responds to the top N request"() {
        when: "A topN query runs with a valid cache hit"
        boolean requestProcessed = handler.handleRequest(requestContext, apiRequest, topNQuery, response)

        then: "Check the cache and return valid json"
        1 * dataCache.get(_) >> "[]"

        then: "Process the Json response"
        1 * response.processResponse(json, topNQuery, _)

        and: "The request is marked as processed"
        requestProcessed
    }

    def "Test handle request on cache hit responds to the time series request"() {
        when: "A timeseries query runs with a valid cache hit"
        boolean requestProcessed = handler.handleRequest(requestContext, apiRequest, timeseriesQuery, response)

        then: "Check the cache and return valid json"
        1 * dataCache.get(_) >> "[]"

        then: "Process the Json response"
        1 * response.processResponse(json, timeseriesQuery, _)

        and: "The request is marked as processed"
        requestProcessed
    }

    def "Test handle request cache miss delegates response to next handler"() {
        when: "A request is sent that has a cache miss"
        boolean requestProcessed = handler.handleRequest(requestContext, apiRequest, groupByQuery, response)

        then: "The cache is checked for a match and misses"
        1 * dataCache.get(_) >> null

        then: "We delegate to the next handler, wrapping in a Caching Response Processor"
        1 * next.handleRequest(requestContext, apiRequest, groupByQuery, _ as CachingResponseProcessor) >> true

        and: "We don't immediately process the response"
        0 * response.processResponse(json, groupByQuery, _)

        and: "The request is marked as processed"
        requestProcessed
    }

    def "Test handle request cache skip delegates response to next handler"() {
        setup:
        RequestContext requestContext = new RequestContext(containerRequestContext, false)

        when: "A request is sent that has a cache miss"
        boolean requestProcessed = handler.handleRequest(requestContext, apiRequest, groupByQuery, response)

        then: "The cache is not checked for a match"
        0 * dataCache.get(_)

        then: "We delegate to the next handler, wrapping in a Caching Response Processor"
        1 * next.handleRequest(requestContext, apiRequest, groupByQuery, _ as CachingResponseProcessor) >> true

        and: "We don't immediately process the response"
        0 * response.processResponse(json, groupByQuery, _)

        and: "The request is marked as processed"
        requestProcessed
    }

    def "Test handle request cache json key error delegates to next handler with cache response processor"() {
        when: "Query that retrieves cache hit with json serialization error"
        boolean requestProcessed = handler.handleRequest(requestContext, apiRequest, groupByQuery, response)

        then: "The cache returns an invalid cache hit"
        1 * dataCache.get(_) >> "...NOT VALID JSON"

        then: "Continue the request to the next handler with a Caching Response Processor"
        1 * next.handleRequest(requestContext, apiRequest, groupByQuery, _ as CachingResponseProcessor) >> true

        then: "The request is marked as processed"
        requestProcessed
    }

    def "Test handle request key parse error delegates to next handler with cache response processor"() {
        setup:
        mapper = Mock(ObjectMapper)
        ObjectWriter writer = Mock(ObjectWriter)
        mapper.writer() >> writer
        handler = Spy(CacheRequestHandler, constructorArgs: [next, dataCache, mapper])

        when: "A request is sent with an invalid cache key"
        boolean requestProcessed = handler.handleRequest(requestContext, apiRequest, groupByQuery, response)

        then: "Error occurs while writing the cache key"
        1 * handler.getKey(_) >> { throw new JsonProcessingException("TestException") }

        then: "Delegate to the next request handler with a Caching Response Processor"
        1 * next.handleRequest(requestContext, apiRequest, groupByQuery, _ as CachingResponseProcessor) >> true

        and: "Request is flagged as processed"
        requestProcessed
    }
}
