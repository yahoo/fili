// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.data.cache.DataCache;
import com.yahoo.bard.webservice.data.cache.TupleDataCache;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.metadata.QuerySigningService;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.CacheV2ResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.LoggingContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;
import com.yahoo.bard.webservice.web.util.QuerySignedCacheService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

/**
 * Request handler to check the cache for a matching request and either return the cached result or send the next
 * handler.
 * <p>
 * It also wraps the response processor so that valid, non-partial JSON responses are cached
 */
public class CacheV2RequestHandler extends BaseDataRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CacheV2RequestHandler.class);

    protected final @NotNull DataRequestHandler next;
    protected final @NotNull TupleDataCache<String, Long, String> dataCache;
    protected final @NotNull QuerySigningService<Long> querySigningService;
    protected final @NotNull QuerySignedCacheService querySignedCacheService;

    /**
     * Build a Cache request handler.
     *
     * @param next  The next handler in the chain
     * @param dataCache  The cache instance
     * @param querySigningService The service to generate query signatures
     * @param querySignedCacheService The service for cache support
     * @param mapper  The mapper for all JSON processing
     */
    @SuppressWarnings("unchecked")
    public CacheV2RequestHandler(
            DataRequestHandler next,
            @NotNull DataCache<?> dataCache,
            QuerySigningService<?> querySigningService,
            QuerySignedCacheService querySignedCacheService,
            ObjectMapper mapper
    ) {
        super(mapper);
        this.next = next;
        this.dataCache = (TupleDataCache<String, Long, String>) dataCache;
        this.querySigningService = (QuerySigningService<Long>) querySigningService;
        this.querySignedCacheService = querySignedCacheService;
    }

    @Override
    public boolean handleRequest(
            final RequestContext context,
            final DataApiRequest request,
            final DruidAggregationQuery<?> druidQuery,
            final ResponseProcessor response
    ) {
        ResponseProcessor nextResponse = response;

        String cacheKey;

        try {
            cacheKey = getKey(druidQuery);
        } catch (JsonProcessingException e) {
            LOG.warn("Cache key cannot be built: ", e);
            return next.handleRequest(context, request, druidQuery, response);
        }

        try {
            if (context.isReadCache()) {
                String cacheResponse = querySignedCacheService.readCache(context, druidQuery);
                if (cacheResponse != null) {
                    RequestLog logCtx = RequestLog.dump();
                    nextResponse.processResponse(
                            mapper.readTree(cacheResponse),
                            druidQuery,
                            new LoggingContext(logCtx)
                    );
                    return true;
                }
            }
        } catch (JsonProcessingException e) {
            LOG.warn("Cache response cannot be read: ", e);
        } catch (Exception e) {
            LOG.warn("Unknown Error processing cache read: ", e);
        }

        // Cached value either doesn't exist or is invalid
        nextResponse = buildResponseProcessor(response, cacheKey);

        return next.handleRequest(context, request, druidQuery, nextResponse);
    }

    /**
     * Extended to allow overriding.
     *
     * @param response The response callback for the request
     * @param cacheKey  The cache key for the request
     *
     * @return the response processor to handle caching.
     */
    protected ResponseProcessor buildResponseProcessor(final ResponseProcessor response, final String cacheKey) {
        return new CacheV2ResponseProcessor(response, cacheKey, dataCache, querySigningService, mapper);
    }

    /**
     * Construct the cache key.
     * Current implementation includes all the fields of the druidQuery besides the context.
     *
     * @param druidQuery  The druid query.
     *
     * @return The cache key as a String.
     * @throws JsonProcessingException if the druid query cannot be serialized to JSON
     */
    protected String getKey(DruidAggregationQuery<?> druidQuery) throws JsonProcessingException {
        JsonNode root = mapper.valueToTree(druidQuery);
        Utils.canonicalize(root , mapper, false);
        return writer.writeValueAsString(root);
    }
}
