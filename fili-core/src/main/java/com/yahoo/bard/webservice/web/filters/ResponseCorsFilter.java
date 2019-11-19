// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.logging.RequestLog;

import java.io.IOException;

import javax.annotation.Priority;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MultivaluedMap;

/**
 * The filter which supports CORS security checking.
 */
@Singleton
@Priority(2)
public class ResponseCorsFilter implements ContainerResponseFilter {

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
            throws IOException {
        RequestLog.startTiming(this);
        MultivaluedMap<String, Object> headers = responseContext.getHeaders();

        String origin = requestContext.getHeaderString("origin");
        if (origin == null || "".equals(origin)) {
            headers.add("Access-Control-Allow-Origin", "*");
        } else {
            headers.add("Access-Control-Allow-Origin", origin);
        }

        String requestedHeaders = requestContext.getHeaderString("access-control-request-headers");
        // allow all requested headers
        headers.add("Access-Control-Allow-Headers", requestedHeaders == null ? "" : requestedHeaders);

        headers.add("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS, PUT, PATCH");
        headers.add("Access-Control-Allow-Credentials", "true");
        RequestLog.stopTiming(this);
    }
}
