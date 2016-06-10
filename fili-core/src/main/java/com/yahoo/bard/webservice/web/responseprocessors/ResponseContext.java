// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.web.DataApiRequest;

import org.joda.time.DateTimeZone;

/**
 * A container for state gathered by the web container and used to handle requests.
 */
public class ResponseContext {

    /**
     * The logging context.
     */
    final private RequestLog logCtx;

    /**
     * The time zone to process into result rows.
     */
    private final DateTimeZone dateTimeZone;

    /**
     * Build a context for a response.
     *
     * @param logCtx  The log snapshot corresponding to this response
     * @param request The API Request that this response context is tied to
     */
    public ResponseContext(RequestLog logCtx, DataApiRequest request) {
        this.logCtx = logCtx;
        this.dateTimeZone = request.getTimeZone();
    }

    public RequestLog getRequestLog() {
        return logCtx;
    }

    public DateTimeZone getDateTimeZone() {
        return dateTimeZone;
    }
}
