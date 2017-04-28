// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;


import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER;
import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.data.cache.DataCache;
import com.yahoo.bard.webservice.data.cache.TupleDataCache;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfo;
import com.yahoo.bard.webservice.metadata.QuerySigningService;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.CacheV2ResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.LoggingContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

import javax.validation.constraints.NotNull;

/**
 * Request handler to check the cache for a matching request and either return the cached result or send the next
 * handler.
 * <p>
 * It also wraps the response processor so that valid, non-partial JSON responses are cached
 */
public class CacheV2RequestHandler extends BaseDataRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CacheV2RequestHandler.class);
    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();
    public static final Meter CACHE_HITS = REGISTRY.meter("queries.meter.cache.hits");
    public static final Meter CACHE_POTENTIAL_HITS = REGISTRY.meter("queries.meter.cache.potential_hits");
    public static final Meter CACHE_MISSES = REGISTRY.meter("queries.meter.cache.misses");
    public static final Meter CACHE_REQUESTS = REGISTRY.meter("queries.meter.cache.total");

    protected final @NotNull DataRequestHandler next;
    protected final @NotNull TupleDataCache<String, Long, String> dataCache;
    protected final @NotNull QuerySigningService<Long> querySigningService;

    /**
     * Build a Cache request handler.
     *
     * @param next  The next handler in the chain
     * @param dataCache  The cache instance
     * @param querySigningService The service to generate query signatures
     * @param mapper  The mapper for all JSON processing
     */
    @SuppressWarnings("unchecked")
    public CacheV2RequestHandler(
            DataRequestHandler next,
            @NotNull DataCache<?> dataCache,
            QuerySigningService<?> querySigningService,
            ObjectMapper mapper
    ) {
        super(mapper);
        this.next = next;
        this.dataCache = (TupleDataCache<String, Long, String>) dataCache;
        this.querySigningService = (QuerySigningService<Long>) querySigningService;
    }

    @Override
    public boolean handleRequest(
            final RequestContext context,
            final DataApiRequest request,
            final DruidAggregationQuery<?> druidQuery,
            final ResponseProcessor response
    ) {
        ResponseProcessor nextResponse = response;

        String cacheKey = null;
        try {
            cacheKey = getKey(druidQuery);

            if (context.isReadCache()) {
                final TupleDataCache.DataEntry<String, Long, String> cacheEntry = dataCache.get(cacheKey);
                CACHE_REQUESTS.mark(1);

                if (cacheEntry != null) {
                    // Make sure that if the optional return value is empty, the statement always evaluates to false
                    // Metadata type needs to be int.
                    if (
                            querySigningService.getSegmentSetId(druidQuery)
                                    .map(id -> Objects.equals(cacheEntry.getMeta(), id))
                                    .orElse(false)
                    ) {
                        try {
                            if (context.getNumberOfOutgoing().decrementAndGet() == 0) {
                                RequestLog.record(new BardQueryInfo(druidQuery.getQueryType().toJson(), true));
                                RequestLog.stopTiming(REQUEST_WORKFLOW_TIMER);
                            }

                            if (context.getNumberOfIncoming().decrementAndGet() == 0) {
                                RequestLog.startTiming(RESPONSE_WORKFLOW_TIMER);
                            }

                            CACHE_HITS.mark(1);
                            RequestLog logCtx = RequestLog.dump();
                            nextResponse.processResponse(
                                    mapper.readTree(cacheEntry.getValue()),
                                    druidQuery,
                                    new LoggingContext(logCtx)
                            );

                            return true;

                        } catch (IOException e) {
                            LOG.warn("Error processing cached value: ", e);
                        }
                    } else {
                        LOG.debug("Cache entry present but invalid for query with id: {}", RequestLog.getId());
                        CACHE_POTENTIAL_HITS.mark(1);
                        CACHE_MISSES.mark(1);
                    }
                } else {
                    CACHE_MISSES.mark(1);
                }
            }
        } catch (Exception e) {
            LOG.warn("Cache key cannot be built: ", e);
        }

        // Cached value either doesn't exist or is invalid
        nextResponse = new CacheV2ResponseProcessor(
                response,
                cacheKey,
                dataCache,
                querySigningService,
                mapper
        );

        return next.handleRequest(context, request, druidQuery, nextResponse);
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
        Utils.omitField(root, "context", mapper);
        return writer.writeValueAsString(root);
    }
}
