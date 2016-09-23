// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.logging.RequestLog;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

/**
 * Filter to return X-Request-ID.
 */
public class RequestIdHeaderFilter implements ContainerResponseFilter {
    public static final String X_REQUEST_ID = "X-Request-ID";

    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) throws IOException {
        response.getHeaders().putSingle(X_REQUEST_ID, RequestLog.getId());
    }
}
