// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import static com.yahoo.bard.webservice.web.handlers.PartialDataRequestHandler.getPartialIntervalsWithDefault;
import static com.yahoo.bard.webservice.web.handlers.VolatileDataRequestHandler.getVolatileIntervalsWithDefault;

import com.yahoo.bard.webservice.data.cache.DataCache;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

/**
 * A response processor which caches the results if appropriate after completing a query.
 */
public class CachingResponseProcessor implements ResponseProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(CachingResponseProcessor.class);

    private final ResponseProcessor next;
    private final String cacheKey;
    private final @NotNull DataCache<String> dataCache;

    protected final ObjectWriter writer;

    /**
     * Constructor.
     *
     * @param next  Next ResponseProcessor in the chain
     * @param cacheKey  Key into which to write a cache entry
     * @param dataCache  The cache into which to write a cache entry
     * @param mapper  An object mapper to use for processing Json
     */
    public CachingResponseProcessor(
            ResponseProcessor next,
            String cacheKey,
            DataCache<String> dataCache,
            ObjectMapper mapper
    ) {
        this.next = next;
        this.cacheKey = cacheKey;
        this.dataCache = dataCache;
        this.writer = mapper.writer();
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
        if (isCacheable()) {
            String valueString = null;
            try {
                valueString = writer.writeValueAsString(json);
                dataCache.set(cacheKey, valueString);
            } catch (Exception e) {
                LOG.warn(
                        "Unable to cache {}value of size: {}",
                        valueString == null ? "null " : "",
                        valueString == null ? "N/A" : valueString.length(),
                        e
                );
            }
        }
        next.processResponse(json, druidQuery, metadata);
    }

    /**
     * A request is cacheable if it does not refer to partial data.
     *
     * @return whether request can be cached
     */
    private boolean isCacheable() {
        SimplifiedIntervalList missingIntervals = getPartialIntervalsWithDefault(getResponseContext());
        SimplifiedIntervalList volatileIntervals = getVolatileIntervalsWithDefault(getResponseContext());

        return missingIntervals.isEmpty() && volatileIntervals.isEmpty();
    }
}
