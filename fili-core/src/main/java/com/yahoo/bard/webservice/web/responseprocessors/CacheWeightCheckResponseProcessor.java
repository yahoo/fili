// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.cache.TupleDataCache;
import com.yahoo.bard.webservice.metadata.QuerySigningService;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A response processor which wraps a timer around the outer most response processor only in the event of an error
 * response.
 */
public class CacheWeightCheckResponseProcessor extends CacheV2ResponseProcessor {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private final ResponseProcessor next;

    private final long maxDruidResponseLengthToCache = SYSTEM_CONFIG.getLongProperty(
            SYSTEM_CONFIG.getPackageVariableName(
                    "druid_max_response_length_to_cache"
            ),
            Long.MAX_VALUE
    );

    /**
     * Constructor.
     *
     * @param next  Next ResponseProcessor in the chain
     * @param cacheKey  Key into which to write a cache entry
     * @param dataCache  The cache into which to write a cache entry
     * @param querySigningService  Service to use for signing the queries in the cache key with their metadata
     * @param mapper  An object mapper to use for processing Json
     */
    public CacheWeightCheckResponseProcessor(
            ResponseProcessor next,
            String cacheKey,
            TupleDataCache<String, Long, String> dataCache,
            QuerySigningService<Long> querySigningService,
            ObjectMapper mapper
    ) {
        super(
                next,
                cacheKey,
                dataCache,
                querySigningService,
                mapper
        );
        this.next = next;
    }

    @Override
    public ResponseContext getResponseContext() {
        return next.getResponseContext();
    }

    @Override
    public FailureCallback getFailureCallback(final DruidAggregationQuery<?> druidQuery) {
        return new FailureCallback() {
            @Override
            public void invoke(Throwable error) {
                if (RequestLog.isRunning(REQUEST_WORKFLOW_TIMER)) {
                    RequestLog.stopTiming(REQUEST_WORKFLOW_TIMER);
                }
                next.getFailureCallback(druidQuery).invoke(error);
            }
        };
    }

    @Override
    public HttpErrorCallback getErrorCallback(final DruidAggregationQuery<?> druidQuery) {
        return new HttpErrorCallback() {
            @Override
            public void invoke(int statusCode, String reason, String responseBody) {
                if (RequestLog.isRunning(REQUEST_WORKFLOW_TIMER)) {
                    RequestLog.stopTiming(REQUEST_WORKFLOW_TIMER);
                }
                next.getErrorCallback(druidQuery).invoke(statusCode, reason, responseBody);
            }
        };
    }
}
