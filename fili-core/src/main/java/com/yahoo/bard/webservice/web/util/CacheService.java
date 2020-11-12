// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.handlers.RequestContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * An interface to assist with caching.
 */
public interface CacheService {
    /**
     * Read cache.
     *
     * @param context The context data from the request processing chain
     * @param druidQuery The query being processed
     *
     * @return Response
     */
    String readCache(
            RequestContext context,
            DruidAggregationQuery<?> druidQuery
    ) throws JsonProcessingException;

    /**
     * Write cache.
     *
     * @param response The response handler
     * @param json Json value to be written to cache as string
     * @param druidQuery The query being processed
     */
    void writeCache(
            ResponseProcessor response,
            JsonNode json,
            DruidAggregationQuery<?> druidQuery
    ) throws JsonProcessingException;

    /**
     * A request is cacheable if it does not refer to partial data.
     *
     * @param response The response handler
     *
     * @return whether request can be cached
     */
    boolean isCacheable(ResponseProcessor response);
}
