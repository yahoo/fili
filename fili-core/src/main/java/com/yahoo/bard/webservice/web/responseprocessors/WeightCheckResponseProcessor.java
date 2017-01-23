// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A response processor which wraps a timer around the outer most response processor only in the event of an error
 * response.
 */
public class WeightCheckResponseProcessor implements ResponseProcessor {

    private final ResponseProcessor next;

    /**
     * Constructor.
     *
     * @param next  The next ResponseProcessor in the chain to call
     */
    public WeightCheckResponseProcessor(ResponseProcessor next) {
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

    @Override
    public void processResponse(JsonNode json, DruidAggregationQuery<?> druidQuery, LoggingContext metadata) {
        next.processResponse(json, druidQuery, metadata);
    }
}
