// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client;

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER;
import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.logging.RequestLog;

/**
 * Callback from the async HTTP client on error.
 */
public interface HttpErrorCallback {
    /**
     * Invoke the error callback code.
     *
     * @param statusCode  Http status code of the error response
     * @param reasonPhrase  The reason for the error. Often the status code description.
     * @param responseBody  The body of the error response
     */
    void invoke(int statusCode, String reasonPhrase, String responseBody);

    /**
     * Stop the request timer, start the response timer, and then invoke the error callback code.
     *
     * @param statusCode  Http status code of the error response
     * @param reasonPhrase  The reason for the error. Often the status code description.
     * @param responseBody  The body of the error response
     */
    default void dispatch(int statusCode, String reasonPhrase, String responseBody) {
        RequestLog.stopTiming(REQUEST_WORKFLOW_TIMER);
        RequestLog.startTiming(RESPONSE_WORKFLOW_TIMER);
        invoke(statusCode, reasonPhrase, responseBody);
    }
}
