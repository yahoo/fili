// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import static com.yahoo.bard.webservice.web.handlers.PartialDataRequestHandler.getPartialIntervalsWithDefault;
import static com.yahoo.bard.webservice.web.handlers.VolatileDataRequestHandler.getVolatileIntervalsWithDefault;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.cache.TupleDataCache;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.metadata.QuerySigningService;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Response;

public class PartialDataV2ResponseProcessor implements FullResponseProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(PartialDataV2ResponseProcessor.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private final long druidUncoveredIntervalLimit = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName(
                    "druid_uncovered_interval_limit"
            ),
            0
    );

    private final ResponseProcessor next;
    private final DataSourceMetadataService dataSourceMetadataService;
    private final String cacheKey;
    private final @NotNull TupleDataCache<String, Long, String> dataCache;
    private final @NotNull QuerySigningService<Long> querySigningService;

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
    public PartialDataV2ResponseProcessor(
            ResponseProcessor next,
            DataSourceMetadataService dataSourceMetadataService,
            String cacheKey,
            TupleDataCache<String, Long, String> dataCache,
            QuerySigningService<Long> querySigningService,
            ObjectMapper mapper
    ) {
        this.next = next;
        this.dataSourceMetadataService = dataSourceMetadataService;
        this.cacheKey = cacheKey;
        this.dataCache = dataCache;
        this.querySigningService = querySigningService;
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
        if (!json.has("status-code")) {
            String message = "status-code is missing from response.";
            LOG.error(message);
            throw new RuntimeException(message);
        }

        if (json.get("status-code").asInt() == Response.Status.OK.getStatusCode()) {
            if (!json.has("X-Druid-Response-Context")) {
                String message = "X-Druid-Response-Context is missing from response.";
                LOG.error(message);
                throw new RuntimeException(message);
            }

            JsonNode druidResponseContext = json.get("X-Druid-Response-Context");
            if (druidResponseContext.has("uncoveredIntervalsOverflowed")) {
                String message = "uncoveredIntervalsOverflowed is missing from X-Druid-Response-Context.";
                LOG.error(message);
                throw new RuntimeException(message);
            }

            if (druidResponseContext.has("uncoveredIntervals")) {
                String message = "uncoveredIntervals is missing from X-Druid-Response-Context.";
                LOG.error(message);
                throw new RuntimeException(message);
            }

            if (druidResponseContext.get("uncoveredIntervalsOverflowed").asBoolean()) {
                String message = ErrorMessageFormat.TOO_MUCH_INTERVAL_MISSING.format(
                        druidUncoveredIntervalLimit,
                        druidUncoveredIntervalLimit
                );
                LOG.error(message);
                throw new RuntimeException(message);
            }

            SimplifiedIntervalList uncovertedIntervals = getUncoveredIntervals(
                    druidResponseContext.get("uncoveredIntervals")
            );

        }
        next.processResponse(json, druidQuery, metadata);
        if (isCacheable()) {
            String valueString = null;
            try {
                valueString = writer.writeValueAsString(json);
                int valueLength = valueString.length();
                if (valueLength <= maxDruidResponseLengthToCache) {
                    dataCache.set(
                            cacheKey,
                            querySigningService.getSegmentSetId(druidQuery).orElse(null),
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
                LOG.warn(
                        "Unable to cache {}value of size: {}",
                        valueString == null ? "null " : "",
                        valueString == null ? "N/A" : valueString.length(),
                        e
                );
            }
        }
    }

    /**
     * Return intervals wrapped inside a SimplifiedIntervalList from a list of strings in JsonNode。
     * <p>
     * For example,
     * "stringList": ["2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z",
     * "2016-11-22T00:00:00.000Z/2016-12-01T00:00:00.000Z"] as a JsonNode
     * becomes
     * [2016-11-22T00:00:00.000Z/2016-12-18T00:00:00.000Z] as a SimplifiedIntervalList.
     *
     * @param uncoveredIntervals  The list of intervals in string in a JsonNode
     *
     * @return intervals wrapped inside a SimplifiedIntervalList
     */
    private static SimplifiedIntervalList getUncoveredIntervals(JsonNode uncoveredIntervals) {
        return new SimplifiedIntervalList(
                StreamSupport.stream(uncoveredIntervals.spliterator(), true)
                        .map(Interval::new)
                        .collect(Collectors.toList())
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

        return missingIntervals.isEmpty() && volatileIntervals.isEmpty();
    }
}
