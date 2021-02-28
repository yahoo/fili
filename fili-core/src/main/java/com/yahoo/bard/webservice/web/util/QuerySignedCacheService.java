// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util;

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER;
import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;
import static com.yahoo.bard.webservice.config.BardFeatureFlag.CACHE_PARTIAL_DATA;
import static com.yahoo.bard.webservice.web.handlers.PartialDataRequestHandler.getPartialIntervalsWithDefault;
import static com.yahoo.bard.webservice.web.handlers.VolatileDataRequestHandler.getVolatileIntervalsWithDefault;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.logging.blocks.BardCacheInfo;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.data.cache.TupleDataCache;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfo;
import com.yahoo.bard.webservice.metadata.QuerySigningService;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.bard.webservice.web.handlers.RequestContext;
import com.yahoo.bard.webservice.web.responseprocessors.CacheV2ResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import com.fasterxml.jackson.databind.JsonNode;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.*;
import java.util.Objects;


/**
 * A utility class to assist with caching.
 */
public class QuerySignedCacheService implements CacheService {

    private static final Logger LOG = LoggerFactory.getLogger(QuerySignedCacheService.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private final long maxDruidResponseLengthToCache = SYSTEM_CONFIG.getLongProperty(
            SYSTEM_CONFIG.getPackageVariableName(
                    "druid_max_response_length_to_cache"
            ),
            Long.MAX_VALUE
    );
    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();
    public static final Meter CACHE_HITS = REGISTRY.meter("queries.meter.cache.hits");
    public static final Meter CACHE_POTENTIAL_HITS = REGISTRY.meter("queries.meter.cache.potential_hits");
    public static final Meter CACHE_MISSES = REGISTRY.meter("queries.meter.cache.misses");
    public static final Meter CACHE_REQUESTS = REGISTRY.meter("queries.meter.cache.total");
    public static final Meter CACHE_SET_FAILURES = REGISTRY.meter("queries.meter.cache.put.failures");
    public static final String LOG_CACHE_READ_FAILURES = "cacheReadFailure";
    public static final String LOG_CACHE_SET_FAILURES = "cacheSetFailure";

    public static final String LOG_CACHE_GET_HIT = "cacheHit";
    public static final String LOG_CACHE_GET_MISS = "cacheMiss";
    public static final String LOG_CACHE_SIGNATURE_MISMATCH = "cacheSignatureMismatch";

    TupleDataCache<String, Long, String> dataCache;
    QuerySigningService<Long> querySigningService;
    ObjectMapper objectMapper;
    ObjectWriter writer;
    /**
     * Constructor.
     *
     * @param dataCache  The cache instance
     * @param querySigningService  The service to generate query signatures
     * @param objectMapper A JSON object mapper, used to parse JSON response
     */
    @Inject
    public QuerySignedCacheService(
            TupleDataCache<String, Long, String> dataCache,
            QuerySigningService<Long> querySigningService,
            ObjectMapper objectMapper
    ) {
        this.dataCache = dataCache;
        this.querySigningService = querySigningService;
        this.objectMapper = objectMapper;
        this.writer = objectMapper.writer();
    }


    @Override
    public String readCache(
            RequestContext context,
            DruidAggregationQuery<?> druidQuery
    ) throws JsonProcessingException {
        String querySignatureHash = String.valueOf(querySigningService.getSegmentSetId(druidQuery).orElse(null));
        final TupleDataCache.DataEntry<String, Long, String> cacheEntry = dataCache.get(getKey(druidQuery));
        CACHE_REQUESTS.mark(1);

        if (cacheEntry != null) {
            if (
                    querySigningService.getSegmentSetId(druidQuery)
                            .filter(id -> Objects.equals(cacheEntry.getMeta(), id))
                            .isPresent()
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
                    BardQueryInfo.getBardQueryInfo().addCacheInfo(
                            CacheV2ResponseProcessor.getMD5Cksum(getKey(druidQuery)),
                            new BardCacheInfo(
                                    LOG_CACHE_GET_HIT,
                                    getKey(druidQuery).length(),
                                    CacheV2ResponseProcessor.getMD5Cksum(getKey(druidQuery)),
                                    querySignatureHash != null
                                            ? CacheV2ResponseProcessor.getMD5Cksum(querySignatureHash)
                                            : null,
                                    cacheEntry.getValue().length()
                            )
                    );
                    return cacheEntry.getValue();

                } catch (Exception e) {
                    LOG.warn("Error processing cached value for key {} with cksum {}",
                            getKey(druidQuery),
                            CacheV2ResponseProcessor.getMD5Cksum(getKey(druidQuery)),
                            e);
                    BardQueryInfo.getBardQueryInfo().addCacheInfo(
                            CacheV2ResponseProcessor.getMD5Cksum(getKey(druidQuery)),
                            new BardCacheInfo(
                                    LOG_CACHE_READ_FAILURES,
                                    getKey(druidQuery).length(),
                                    CacheV2ResponseProcessor.getMD5Cksum(getKey(druidQuery)),
                                    querySignatureHash != null
                                            ? CacheV2ResponseProcessor.getMD5Cksum(querySignatureHash)
                                            : null,
                                    0
                            )
                    );
                }
            } else {
                LOG.debug("Cache entry present but invalid for query with id: {}", RequestLog.getId());
                CACHE_POTENTIAL_HITS.mark(1);
                CACHE_MISSES.mark(1);
                BardQueryInfo.getBardQueryInfo().addCacheInfo(
                        CacheV2ResponseProcessor.getMD5Cksum(getKey(druidQuery)),
                        new BardCacheInfo(
                                LOG_CACHE_SIGNATURE_MISMATCH,
                                getKey(druidQuery).length(),
                                CacheV2ResponseProcessor.getMD5Cksum(getKey(druidQuery)),
                                querySignatureHash != null
                                        ? CacheV2ResponseProcessor.getMD5Cksum(querySignatureHash)
                                        : null,
                                0
                        )
                );
            }
        } else {
            CACHE_MISSES.mark(1);
            BardQueryInfo.getBardQueryInfo().addCacheInfo(
                    CacheV2ResponseProcessor.getMD5Cksum(getKey(druidQuery)),
                    new BardCacheInfo(
                            LOG_CACHE_GET_MISS,
                            getKey(druidQuery).length(),
                            CacheV2ResponseProcessor.getMD5Cksum(getKey(druidQuery)),
                            querySignatureHash != null
                                    ? CacheV2ResponseProcessor.getMD5Cksum(querySignatureHash)
                                    : null,
                            0
                    )
            );
        }
        return null;
    }

    @Override
    public void writeCache(
            ResponseProcessor response,
            JsonNode json,
            DruidAggregationQuery<?> druidQuery
            ) throws JsonProcessingException {
        String querySignatureHash = String.valueOf(querySigningService.getSegmentSetId(druidQuery).orElse(null));
        if (CACHE_PARTIAL_DATA.isOn() || isCacheable(response)) {
            String cacheKey = getKey(druidQuery);
            String valueString = null;
            try {
                valueString = writer.writeValueAsString(json);
                int valueLength = valueString.length();
                if (valueLength <= maxDruidResponseLengthToCache) {
                    dataCache.set(
                            cacheKey,
                            Long.parseLong(
                                    String.valueOf(
                                            querySigningService.getSegmentSetId(druidQuery).orElse(null)
                                    )
                            ),
                            valueString
                    );
                } else {
                    LOG.debug(
                            "Response not cached. Length of {} exceeds max value length of {}",
                            valueLength,
                            maxDruidResponseLengthToCache
                    );
                }
            } catch (Exception e) {
                //mark and log the cache put failure
                CACHE_SET_FAILURES.mark(1);
                BardQueryInfo.getBardQueryInfo().incrementCountCacheSetFailures();
                BardQueryInfo.getBardQueryInfo().addCacheInfo(
                        CacheV2ResponseProcessor.getMD5Cksum(cacheKey),
                        new BardCacheInfo(
                                LOG_CACHE_SET_FAILURES,
                                cacheKey.length(),
                                CacheV2ResponseProcessor.getMD5Cksum(cacheKey),
                                querySignatureHash != null
                                        ? CacheV2ResponseProcessor.getMD5Cksum(querySignatureHash)
                                        : null,
                                valueString != null ? valueString.length() : 0
                        )
                );
                LOG.warn(
                        "Unable to cache {}value of size: {} with cksum: {}",
                        valueString == null ? "null " : "",
                        valueString == null ? "N/A" : valueString.length(),
                        CacheV2ResponseProcessor.getMD5Cksum(cacheKey),
                        e
                );
            }
        }
    }


    @Override
    public boolean isCacheable(ResponseProcessor response) {
        SimplifiedIntervalList missingIntervals = getPartialIntervalsWithDefault(response.getResponseContext());
        SimplifiedIntervalList volatileIntervals = getVolatileIntervalsWithDefault(response.getResponseContext());

        return missingIntervals.isEmpty() && volatileIntervals.isEmpty();
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
    protected String getKey(DruidAggregationQuery<?> druidQuery)
            throws JsonProcessingException {
        JsonNode root = objectMapper.valueToTree(druidQuery);
        Utils.canonicalize(root, objectMapper, false);
        return writer.writeValueAsString(root);
    }
}
