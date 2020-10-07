// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util;

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER;
import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.data.cache.TupleDataCache;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfo;
import com.yahoo.bard.webservice.metadata.QuerySigningService;
import com.yahoo.bard.webservice.web.handlers.RequestContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;


/**
 * A static utility class for assisting with caching.
 */
public class CacheService {

    private static final Logger LOG = LoggerFactory.getLogger(CacheService.class);
    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();
    public static final Meter CACHE_HITS = REGISTRY.meter("queries.meter.cache.hits");
    public static final Meter CACHE_POTENTIAL_HITS = REGISTRY.meter("queries.meter.cache.potential_hits");
    public static final Meter CACHE_MISSES = REGISTRY.meter("queries.meter.cache.misses");
    public static final Meter CACHE_REQUESTS = REGISTRY.meter("queries.meter.cache.total");


    /**
     * Read cache.
     *
     * @param context context
     * @param dataCache data cache
     * @param druidQuery druid query
     * @param querySigningService Query signing service
     * @param cacheKey cache key
     * @return Response
     */
    public static String readCache(
            RequestContext context,
            TupleDataCache<String, Long, String> dataCache,
            QuerySigningService<Long> querySigningService,
            DruidAggregationQuery<?> druidQuery,
            String cacheKey
    ) {
        final TupleDataCache.DataEntry<String, Long, String> cacheEntry = dataCache.get(cacheKey);
        CACHE_REQUESTS.mark(1);

        if (cacheEntry != null) {
            if (
                    querySigningService.getSegmentSetId(druidQuery)
                            .map(id -> Objects.equals(cacheEntry.getMeta(), id))
                            .orElse(false)
            ) {
                try {
                    if (context.getNumberOfOutgoing().decrementAndGet() == 0) {
                        RequestLog.stopTiming(REQUEST_WORKFLOW_TIMER);
                    }

                    if (context.getNumberOfIncoming().decrementAndGet() == 0) {
                        RequestLog.startTiming(RESPONSE_WORKFLOW_TIMER);
                    }
                    CACHE_HITS.mark(1);
                    BardQueryInfo.getBardQueryInfo().incrementCountCacheHits();
                    return cacheEntry.getValue();

                } catch (Exception e) {
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
        return null;
    }

    /**
     * Check weight limit query.
     *
     * @param response response
     * @param druidQuery druid query
     * @param rowCount row count
     * @param queryRowLimit query row limit
     */
    public static void checkWeightLimitQuery(
            ResponseProcessor response,
            DruidAggregationQuery<?> druidQuery,
            int rowCount,
            long queryRowLimit
    ) {
        String reason = String.format(
                ErrorMessageFormat.WEIGHT_CHECK_FAILED.logFormat(rowCount, queryRowLimit),
                rowCount,
                queryRowLimit
        );
        String description = ErrorMessageFormat.WEIGHT_CHECK_FAILED.format();

        LOG.debug(reason);
        response.getErrorCallback(druidQuery).dispatch(
                507, //  Insufficient Storage
                reason,
                description
        );
    }
}
