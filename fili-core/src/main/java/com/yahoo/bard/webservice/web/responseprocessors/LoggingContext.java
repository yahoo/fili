// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import com.yahoo.bard.webservice.logging.RequestLog;

/**
 * A container for RequestLog.
 */
public class LoggingContext {

    /**
     * The logging context.
     */
    final private RequestLog logCtx;

    /**
     * Build a container for RequestLog.
     *
     * @param logCtx  The log snapshot corresponding to this response
     */
    public LoggingContext(RequestLog logCtx) {
        this.logCtx = logCtx;
    }

    public RequestLog getRequestLog() {
        return logCtx;
    }
}
