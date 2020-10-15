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
import com.yahoo.bard.webservice.web.util.CacheService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


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

    protected final @NotNull CacheService cacheService;

    /**
     * Build a cache weight checking request handler.
     *
     * @param next  The request handler to delegate the request to.
     * @param webService  The web service to use for weight checking
     * @param queryWeightUtil  A provider which measures estimated weight against allowed weights.
     * @param mapper  A JSON object mapper, used to parse the JSON response from the weight check.
     * @param cacheService The service for cache support
     */
    public CacheWeightCheckRequestHandler(
            DataRequestHandler next,
            DruidWebService webService,
            QueryWeightUtil queryWeightUtil,
            ObjectMapper mapper,
            CacheService cacheService
    ) {
        super(
                next,
                webService,
                queryWeightUtil,
                mapper
        );
        this.cacheService = (CacheService) cacheService;
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

        SuccessCallback classicCallback = super.buildSuccessCallback(
                context,
                request,
                druidQuery,
                weightCheckResponse,
                queryRowLimit
        );
        HttpErrorCallback error = response.getErrorCallback(druidQuery);
        FailureCallback failure = response.getFailureCallback(druidQuery);

        SuccessCallback cahingSuccessCallback = buildCacheSuccessCallback(
                classicCallback,
                cacheService,
                druidQuery,
                weightCheckResponse
        );
        // No possible cache hit, so just run the query and save to cache
        if (!context.isReadCache()) {
            webService.postDruidQuery(context, cahingSuccessCallback, error, failure, weightEvaluationQuery);
            return true;
        }


        try {
            String cacheResponse = cacheService.readCache(context, druidQuery);
            // There is a cache hit, so no more cache interactions are necessary.

            if (cacheResponse != null) {
                JsonNode rootNode = mapper.readTree(cacheResponse);
                classicCallback.invoke(rootNode);
                return true;
            }
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        webService.postDruidQuery(context, cahingSuccessCallback, error, failure, weightEvaluationQuery);
        return true;
    }

    /**
     * Build a callback that writes to the cache if request wasn't already cached. It then delegates to the
     * WeightCheckRequestHandler callback which continues the original request or refuses it with an
     * HTTP INSUFFICIENT_STORAGE (507)status based on the cardinality of the requester 's query as
     * measured by the weight check query.
     *
     * @param successCallback  The weight check success callback
     * @param cacheService The service for cache support
     * @param druidQuery  The query being processed
     * @param response  The response handler
     *
     * @return The callback handler for the weight request
     */
    protected SuccessCallback buildCacheSuccessCallback(
            SuccessCallback successCallback,
            CacheService cacheService,
            DruidAggregationQuery druidQuery,
            ResponseProcessor response
    ) {
        return new SuccessCallback() {
            @Override
            public void invoke(JsonNode jsonResult) {
                // send the response to the user before waiting to cache
                successCallback.invoke(jsonResult);
                try {
                    cacheService.writeCache(response, jsonResult, druidQuery);
                } catch (JsonProcessingException e) {
                    // Warn on cache write exception only
                }
            }
        };
    }
}
