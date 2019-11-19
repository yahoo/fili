// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Response processing can result in failure or error or on success will receive a JSON document representing the result
 * set for the query.
 */
public interface ResponseProcessor {
    /**
     * The response context allows state to be injected from construction and visible across response processor
     * layers as necessary.
     *
     * @return The context data
     */
    ResponseContext getResponseContext();

    /**
     * Callback handler for unexpected failures.
     *
     * @param query  The query associated with this failure
     *
     * @return The callback handler
     */
    FailureCallback getFailureCallback(DruidAggregationQuery<?> query);

    /**
     * Callback for handling http errors.
     *
     * @param query  The query associated with this error
     *
     * @return The callback handler
     */
    HttpErrorCallback getErrorCallback(DruidAggregationQuery<?> query);

    /**
     * Process the response json and respond to the original web request.
     *
     * @param json  The json representing a druid data response
     * @param query  The query with the schema for processing this response
     * @param metadata  The LoggingContext to use
     */
    void processResponse(JsonNode json, DruidAggregationQuery<?> query, LoggingContext metadata);
}
