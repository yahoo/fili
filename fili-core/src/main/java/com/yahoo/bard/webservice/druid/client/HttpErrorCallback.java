// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client;

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER;
import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.logging.RequestLog;

/**
 * Callback from the async HTTP client on error.
 */
public interface HttpErrorCallback {
    void invoke(int statusCode, String reasonPhrase, String responseBody);

    default void dispatch(int statusCode, String reasonPhrase, String responseBody) {
        RequestLog.stopTiming(REQUEST_WORKFLOW_TIMER);
        RequestLog.startTiming(RESPONSE_WORKFLOW_TIMER);
        invoke(statusCode, reasonPhrase, responseBody);
    }
}
