// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client;

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER;
import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.logging.RequestLog;

/**
 * Callback from the async HTTP client on error.
 */
public interface FailureCallback {
    /**
     * Invoke the failure callback code.
     *
     * @param error  The error that caused the failure
     */
    void invoke(Throwable error);

    /**
     * Stop the request timer, start the response timer, and then invoke the failure callback code.
     *
     * @param error  The error that caused the failure
     */
    default void dispatch(Throwable error) {
        RequestLog.stopTiming(REQUEST_WORKFLOW_TIMER);
        RequestLog.startTiming(RESPONSE_WORKFLOW_TIMER);
        invoke(error);
    }
}
