// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfo;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.WeightCheckResponseProcessor;
import com.yahoo.bard.webservice.web.util.QueryWeightUtil;
import com.yahoo.bard.webservice.data.cache.DataCache;
import com.yahoo.bard.webservice.web.util.CacheService;
import com.yahoo.bard.webservice.util.Utils;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.yahoo.bard.webservice.data.cache.TupleDataCache;
import com.yahoo.bard.webservice.metadata.QuerySigningService;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

/**
 * Cache Weight check request handler determines whether a request should be processed based on estimated query cost.
 * It also checks the cache for a matching request, else writes it to the cache.
 * <ul>
 *     <li>If the dimensions of the query are sufficiently low cardinality, the request is allowed.
 *     <li>Otherwise, send a simplified version of the query to druid asynchronously to measure the cardinality of the
 * results.
 *     <li>If the cost is too high, return an error, otherwise subsequently submit the data request.
 * </ul>
 */
public class CacheWeightCheckRequestHandler extends WeightCheckRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(WeightCheckRequestHandler.class);


    protected final @NotNull TupleDataCache<String, Long, String> dataCache;
    protected final @NotNull QuerySigningService<Long> querySigningService;

    /**
     * Build a cache weight checking request handler.
     *
     * @param next  The request handler to delegate the request to.
     * @param webService  The web service to use for weight checking
     * @param queryWeightUtil  A provider which measures estimated weight against allowed weights.
     * @param mapper  A JSON object mapper, used to parse the JSON response from the weight check.
     * @param dataCache The cache instance
     * @param querySigningService The service to generate query signatures
     */
    public CacheWeightCheckRequestHandler(
            DataRequestHandler next,
            DruidWebService webService,
            QueryWeightUtil queryWeightUtil,
            ObjectMapper mapper,
            @NotNull DataCache<?> dataCache,
            QuerySigningService<?> querySigningService
    ) {
        super(
                next,
                webService,
                queryWeightUtil,
                mapper
        );
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
        // Heuristic test to let requests with very low estimated cardinality directly through
        if (queryWeightUtil.skipWeightCheckQuery(druidQuery)) {
            return next.handleRequest(context, request, druidQuery, response);
        }

        BardQueryInfo.getBardQueryInfo().incrementCountWeightCheck();
        final WeightCheckResponseProcessor weightCheckResponse =
                new WeightCheckResponseProcessor(response);
        final DruidAggregationQuery<?> weightEvaluationQuery = queryWeightUtil.makeWeightEvaluationQuery(druidQuery);
        Granularity granularity = druidQuery.getInnermostQuery().getGranularity();
        final long queryRowLimit = queryWeightUtil.getQueryWeightThreshold(granularity);

        try {
            LOG.debug("Weight query {}", writer.writeValueAsString(weightEvaluationQuery));
        } catch (JsonProcessingException e) {
            LOG.warn("Weight Query json exception:", e);
        }

        CacheService cacheService = new CacheService();
        String cacheKey = null;
        try {
            cacheKey = getKey(druidQuery, mapper, writer);
        } catch (JsonProcessingException e) {
            LOG.warn("Cache key cannot be built: ", e);
        }

        WeightCheckRequestHandler weightCheckRequestHandler =
                new WeightCheckRequestHandler(next, webService, queryWeightUtil, mapper);
        SuccessCallback weightQuerySuccess = null;
        // Construct success callback based on cache hit or cache miss
        if (context.isReadCache()) {
            String cacheResponse =
                    cacheService.readCache(context, dataCache, querySigningService, druidQuery, cacheKey);
            // Cache hit
            if (cacheResponse != null) {
                weightQuerySuccess = buildCacheSuccessCallback(
                        context,
                        request,
                        druidQuery,
                        weightCheckResponse,
                        queryRowLimit,
                        cacheKey,
                        cacheService,
                        weightCheckRequestHandler,
                        false
                );
            }
        } else {
            // Write to cache
            weightQuerySuccess = buildCacheSuccessCallback(
                    context,
                    request,
                    druidQuery,
                    weightCheckResponse,
                    queryRowLimit,
                    cacheKey,
                    cacheService,
                    weightCheckRequestHandler,
                    true
            );
        }
        HttpErrorCallback error = response.getErrorCallback(druidQuery);
        FailureCallback failure = response.getFailureCallback(druidQuery);
        webService.postDruidQuery(context, weightQuerySuccess, error, failure, weightEvaluationQuery);
        return true;
    }

    /**
     * Build a callback that writes to the cache if request wasn't already cached. It then delegates to the
     * WeightCheckRequestHandler callback which continues the original request or refuses it with an
     * HTTP INSUFFICIENT_STORAGE (507)status based on the cardinality of the requester 's query as
     * measured by the weight check query.
     *
     * @param context  The context data from the request processing chain
     * @param request  The API request itself
     * @param druidQuery  The query being processed
     * @param response  The response handler
     * @param queryRowLimit  The number of aggregating lines allowed
     * @param cacheKey Key into which to write a cache entry
     * @param cacheService CacheService object
     * @param weightCheckRequestHandler WeightCheckRequestHandler object
     * @param writeCache Read or Write cache flag
     *
     * @return The callback handler for the weight request
     */
    protected SuccessCallback buildCacheSuccessCallback(
            final RequestContext context,
            final DataApiRequest request,
            final DruidAggregationQuery<?> druidQuery,
            final ResponseProcessor response,
            final long queryRowLimit,
            final String cacheKey,
            final CacheService cacheService,
            final WeightCheckRequestHandler weightCheckRequestHandler,
            final Boolean writeCache
    ) {
        return new SuccessCallback() {
            SuccessCallback superSuccessCallback = weightCheckRequestHandler.buildSuccessCallback(
                    context,
                    request,
                    druidQuery,
                    response,
                    queryRowLimit
            );
            @Override
            public void invoke(JsonNode jsonResult) {
                if (writeCache) {
                    cacheService.writeCache(
                            response,
                            jsonResult,
                            dataCache,
                            druidQuery,
                            querySigningService,
                            cacheKey,
                            writer
                    );
                }
                superSuccessCallback.invoke(jsonResult);
            }
        };
    }

    /**
     * Construct the cache key.
     * Current implementation includes all the fields of the druidQuery besides the context.
     *
     * @param druidQuery  The druid query.
     * @param mapper Mapper
     * @param writer Writer
     *
     * @return The cache key as a String.
     * @throws JsonProcessingException if the druid query cannot be serialized to JSON
     */
    protected String getKey(DruidAggregationQuery<?> druidQuery, ObjectMapper mapper, ObjectWriter writer)
            throws JsonProcessingException {
        JsonNode root = mapper.valueToTree(druidQuery);
        Utils.canonicalize(root , mapper, false);
        return writer.writeValueAsString(root);
    }
}
