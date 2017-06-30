// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER;
import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.data.cache.TupleDataCache;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfo;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.DruidJsonRequestContentKeys;
import com.yahoo.bard.webservice.web.responseprocessors.DruidJsonResponseContentKeys;
import com.yahoo.bard.webservice.web.responseprocessors.EtagCacheResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

/**
 * Request handler to check the cache for a matching eTag.
 * <p>
 * If a query is cached, the etag of the query is retrieved from the cache and is injected into query request header
 * <p>
 * Etag-based cache mechanism relies entirely on Druid's etags to determine a cache hit or miss.
 */
public class EtagCacheRequestHandler extends BaseDataRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(EtagCacheRequestHandler.class);
    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();
    private static final Meter CACHE_HITS = REGISTRY.meter("queries.meter.cache.hits");
    private static final Meter CACHE_MISSES = REGISTRY.meter("queries.meter.cache.misses");
    private static final Meter CACHE_REQUESTS = REGISTRY.meter("queries.meter.cache.total");

    protected final DataRequestHandler next;
    protected final TupleDataCache<String, String, String> dataCache;

    /**
     * Build a Cache request handler.
     *
     * @param next  The next handler in the chain
     * @param dataCache  The cache instance
     * @param mapper  The mapper for all JSON processing
     */
    public EtagCacheRequestHandler(
            @NotNull DataRequestHandler next,
            @NotNull TupleDataCache<String, String, String> dataCache,
            @NotNull ObjectMapper mapper
    ) {
        super(mapper);
        this.next = next;
        this.dataCache = dataCache;
    }

    @Override
    public boolean handleRequest(
            RequestContext context,
            DataApiRequest request,
            DruidAggregationQuery<?> druidQuery,
            ResponseProcessor response
    ) {
        ResponseProcessor nextResponse = response;

        try {
            String cacheKey = null;
            if (context.isReadCache()) {
                cacheKey = getKey(druidQuery);
                final TupleDataCache.DataEntry<String, String , String> cacheEntry = dataCache.get(cacheKey);
                CACHE_REQUESTS.mark(1);

                String eTagInRequest = DruidJsonRequestContentKeys.ETAG.getName();
                if (cacheEntry != null) { // Current query is in data cache
                    // Insert "If-None-Match" header into RequestContext; the value is etag of the corresponding cache
                    context.getHeaders().putSingle(
                            eTagInRequest,
                            mapper.readTree(cacheEntry.getValue())
                                    .get(DruidJsonResponseContentKeys.ETAG.getName())
                                    .asText()
                            );

                    if (context.getNumberOfOutgoing().decrementAndGet() == 0) {
                        RequestLog.record(new BardQueryInfo(druidQuery.getQueryType().toJson(), true));
                        RequestLog.stopTiming(REQUEST_WORKFLOW_TIMER);
                    }

                    if (context.getNumberOfIncoming().decrementAndGet() == 0) {
                        RequestLog.startTiming(RESPONSE_WORKFLOW_TIMER);
                    }

                    CACHE_HITS.mark(1);
                } else { // Current query is not in data cache
                    // Insert "If-None-Match" header into RequestContext; the value a random pre-defined string
                    context.getHeaders().putSingle(
                            eTagInRequest,
                            DruidJsonRequestContentKeys.NON_EXISTING_ETAG_VALUE.getName()
                    );

                    CACHE_MISSES.mark(1);
                }
            }

            nextResponse = new EtagCacheResponseProcessor(
                    response,
                    cacheKey,
                    dataCache,
                    mapper
            );
        } catch (Exception e) {
            LOG.warn("Cache key cannot be built: ", e);
        }

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
    private String getKey(DruidAggregationQuery<?> druidQuery) throws JsonProcessingException {
        JsonNode root = mapper.valueToTree(druidQuery);
        Utils.omitField(root, "context", mapper);
        return writer.writeValueAsString(root);
    }
}
