// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import static com.yahoo.bard.webservice.config.BardFeatureFlag.CACHE_PARTIAL_DATA;
import static com.yahoo.bard.webservice.web.handlers.PartialDataRequestHandler.getPartialIntervalsWithDefault;
import static com.yahoo.bard.webservice.web.handlers.VolatileDataRequestHandler.getVolatileIntervalsWithDefault;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.cache.TupleDataCache;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.blocks.BardCacheInfo;
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfo;
import com.yahoo.bard.webservice.metadata.QuerySigningService;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.util.QuerySignedCacheService;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;

import javax.validation.constraints.NotNull;
import javax.xml.bind.DatatypeConverter;

/**
 * A response processor which caches the results if appropriate after completing a query.
 */
public class CacheV2ResponseProcessor implements ResponseProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(CacheV2ResponseProcessor.class);

    protected static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    protected static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();
    public static final Meter CACHE_SET_FAILURES = REGISTRY.meter("queries.meter.cache.put.failures");

    private final long maxDruidResponseLengthToCache = SYSTEM_CONFIG.getLongProperty(
            SYSTEM_CONFIG.getPackageVariableName(
                    "druid_max_response_length_to_cache"
            ),
            Long.MAX_VALUE
    );

    protected final ResponseProcessor next;
    protected final String cacheKey;
    protected final String cacheKeyChecksum;
    protected final @NotNull TupleDataCache<String, Long, String> dataCache;
    protected final @NotNull QuerySigningService<Long> querySigningService;

    protected final ObjectWriter writer;

    /**
     * Constructor.
     *
     * @param next  Next ResponseProcessor in the chain
     * @param cacheKey  Key into which to write a cache entry
     * @param dataCache  The cache into which to write a cache entry
     * @param querySigningService  Service to use for signing the queries in the cache key with their metadata
     * @param mapper  An object mapper to use for processing Json
     */
    public CacheV2ResponseProcessor(
            ResponseProcessor next,
            String cacheKey,
            TupleDataCache<String, Long, String> dataCache,
            QuerySigningService<Long> querySigningService,
            ObjectMapper mapper
    ) {
        this.next = next;
        this.cacheKey = cacheKey;
        this.dataCache = dataCache;
        this.querySigningService = querySigningService;
        this.writer = mapper.writer();
        this.cacheKeyChecksum = getMD5Checksum(cacheKey);
    }

    @Override
    public ResponseContext getResponseContext() {
        return next.getResponseContext();
    }

    @Override
    public FailureCallback getFailureCallback(DruidAggregationQuery<?> druidQuery) {
        return next.getFailureCallback(druidQuery);
    }

    @Override
    public HttpErrorCallback getErrorCallback(DruidAggregationQuery<?> druidQuery) {
        return next.getErrorCallback(druidQuery);
    }

    @Override
    public void processResponse(JsonNode json, DruidAggregationQuery<?> druidQuery, LoggingContext metadata) {
        // Capture the query signature for the request as we're about to process it
        Long querySignature = querySigningService.getSegmentSetId(druidQuery).orElse(null);

        // First dispatch message to user
        next.processResponse(json, druidQuery, metadata);

        // Then try to cache
        if (isCacheable()) {
            String valueString;
            try {
                valueString = writer.writeValueAsString(json);
            } catch (JsonProcessingException e) {
                handleException(e, querySignature);
                return;
            }

            try {
                int valueLength = valueString.length();
                if (valueLength <= maxDruidResponseLengthToCache) {
                    dataCache.set(
                            cacheKey,
                            querySignature,
                            valueString
                    );
                    afterCache();
                } else {
                    LOG.debug(
                            "Response not cached for query with key checksum {}." +
                                    "Length of {} exceeds max value length of {}",
                            cacheKeyChecksum,
                            valueLength,
                            maxDruidResponseLengthToCache
                    );
                }
            } catch (Exception e) {
                handleException(e, querySignature, valueString);
            }
        }
    }

    private void handleException(Exception e, Long querySignature) {
        //mark and log the cache put failure
        String querySignatureHash = String.valueOf(querySignature);

        CACHE_SET_FAILURES.mark(1);
        BardQueryInfo.incrementCountCacheSetFailures();
        BardQueryInfo.addCacheInfo(cacheKeyChecksum,
                new BardCacheInfo(
                        QuerySignedCacheService.LOG_CACHE_SET_FAILURES,
                        cacheKey.length(),
                        cacheKeyChecksum,
                        querySignature != null
                                ? CacheV2ResponseProcessor.getMD5Checksum(querySignatureHash)
                                : null,
                        0
                )
        );
        LOG.warn("Unable to cache {} value of size: {} and key checksum: {} ", "null ", "N/A", cacheKeyChecksum, e);
    }
    private void handleException(Exception e, Long querySignature, String valueString) {
        //mark and log the cache put failure
        String querySignatureHash = String.valueOf(querySignature);

        CACHE_SET_FAILURES.mark(1);
        BardQueryInfo.incrementCountCacheSetFailures();
        BardQueryInfo.addCacheInfo(cacheKeyChecksum,
                new BardCacheInfo(
                        QuerySignedCacheService.LOG_CACHE_SET_FAILURES,
                        cacheKey.length(),
                        cacheKeyChecksum,
                        querySignature != null
                                ? CacheV2ResponseProcessor.getMD5Checksum(querySignatureHash)
                                : null,
                        valueString != null ? valueString.length() : 0
                )
        );
        LOG.warn(
                "Unable to cache {} value of size: {} and key checksum: {} ",
                valueString == null ? "null " : "",
                valueString == null ? "N/A" : valueString.length(),
                getMD5Checksum(cacheKey),
                e
        );
    }

    /**
     * A request is cacheable if it does not refer to partial data.
     *
     * @return whether request can be cached
     */
    protected boolean isCacheable() {

        SimplifiedIntervalList missingIntervals = getPartialIntervalsWithDefault(getResponseContext());
        SimplifiedIntervalList volatileIntervals = getVolatileIntervalsWithDefault(getResponseContext());

        // Moved from external check to inside this method
        return missingIntervals.isEmpty() && volatileIntervals.isEmpty() || CACHE_PARTIAL_DATA.isOn();
    }

    /**
     * Generate the Checksum of cacheKey using MD5 algorithm.
     * @param cacheKey cache key
     *
     * @return String representation of the checksum
     */
    public static String getMD5Checksum(String cacheKey) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] hash = digest.digest(cacheKey.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hash); // make it readable
        } catch (NoSuchAlgorithmException ex) {
            String msg = "Unable to initialize hash generator with default MD5 algorithm ";
            LOG.warn(msg, ex);
            throw new RuntimeException(msg, ex);
        } catch (Exception exception) {
            String msg = "Failed to generate checksum for cache key";
            LOG.warn(msg, exception);
            throw new RuntimeException(msg, exception);
        }
    }

    /**
     * Converts bytes array to the hex String.
     * @param hash array of bytes to be converted in Hex
     *
     * @return String representation of the checksum
     */
    public static String  bytesToHex(byte[] hash) {
        return DatatypeConverter.printHexBinary(hash)
                .toLowerCase(Locale.ENGLISH);
    }

    /**
     * Path for cacheStore implementation.
     */
    protected void afterCache() {
    }
}
