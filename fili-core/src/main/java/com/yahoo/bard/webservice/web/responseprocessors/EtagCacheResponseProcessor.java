// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_MODIFIED;
import static javax.ws.rs.core.Response.Status.OK;

import com.yahoo.bard.webservice.data.cache.TupleDataCache;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.validation.constraints.NotNull;

/**
 * A response processor which caches the results if appropriate after completing a query according to etag value.
 */
public class EtagCacheResponseProcessor implements FullResponseProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(EtagCacheResponseProcessor.class);

    private final ObjectMapper mapper;
    private final ObjectWriter writer;
    private final ResponseProcessor next;
    private final String cacheKey;
    private final TupleDataCache<String, String, String> dataCache;

    /**
     * Constructor.
     *
     * @param next  Next ResponseProcessor in the chain
     * @param cacheKey  Key into which to write a cache entry
     * @param dataCache  The cache into which to write a cache entry
     * @param mapper  An object mapper to use for processing Json
     */
    public EtagCacheResponseProcessor(
            @NotNull ResponseProcessor next,
            @NotNull String cacheKey,
            @NotNull TupleDataCache<String, String, String> dataCache,
            @NotNull ObjectMapper mapper
    ) {
        this.next = next;
        this.cacheKey = cacheKey;
        this.dataCache = dataCache;
        this.mapper = mapper;
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
        // make sure JSON response comes with status code
        if (!json.has(DruidJsonResponseContentKeys.STATUS_CODE.getName())) {
            logAndGetErrorCallback(ErrorMessageFormat.STATUS_CODE_MISSING_FROM_RESPONSE.format(), druidQuery);
            return;
        }

        int statusCode = json.get(DruidJsonResponseContentKeys.STATUS_CODE.getName()).asInt();
        // If response is a NOT_MODIFIED, get response body from cache and inject it into JsonNode of the next
        // response processor
        if (statusCode == NOT_MODIFIED.getStatusCode()) {
            try {
                ((ObjectNode) json).set(
                        DruidJsonResponseContentKeys.RESPONSE.getName(),
                        mapper.readTree(dataCache.getDataValue(cacheKey))
                );
            } catch (IOException ioe) {
                logAndGetErrorCallback(ioe.getLocalizedMessage(), druidQuery);
                return;
            }
        } else if (statusCode == OK.getStatusCode()) { // If response is a OK, cache it, including etag
            // make sure JSON response comes with etag
            if (!json.has(DruidJsonResponseContentKeys.ETAG.getName())) {
                LOG.warn(ErrorMessageFormat.ETAG_MISSING_FROM_RESPONSE.format());
            } else {
                try {
                    dataCache.set(
                            cacheKey,
                            json.get(DruidJsonResponseContentKeys.ETAG.getName()).asText(),
                            writer.writeValueAsString(json.get(DruidJsonResponseContentKeys.RESPONSE.getName()))
                    );
                } catch (JsonProcessingException exception) {
                    logAndGetErrorCallback("Unable to parse JSON response while caching", druidQuery);
                    return;
                }
            }
        }

        ((ObjectNode) json).put(
                DruidJsonResponseContentKeys.CACHED_RESPONSE.getName(),
                statusCode == NOT_MODIFIED.getStatusCode()
        );

        if (next instanceof FullResponseProcessor) {
            next.processResponse(json, druidQuery, metadata);
        } else {
            next.processResponse(json.get(DruidJsonResponseContentKeys.RESPONSE.getName()), druidQuery, metadata);
        }
    }

    /**
     * Logs and gets error call back on the response with the provided error message.
     *
     * @param message  The error message passed to the logger and the exception
     * @param query  The query with the schema for processing this response
     */
    private void logAndGetErrorCallback(String message, DruidAggregationQuery<?> query) {
        LOG.error(message);
        getErrorCallback(query).dispatch(
                INTERNAL_SERVER_ERROR.getStatusCode(),
                ErrorMessageFormat.INTERNAL_SERVER_ERROR_REASON_PHRASE.format(),
                message
        );
    }
}
