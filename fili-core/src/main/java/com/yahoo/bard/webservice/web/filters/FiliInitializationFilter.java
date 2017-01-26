// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.logging.RequestLogUtils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.regex.Pattern;

import javax.annotation.Priority;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;

/**
 * Starts/stops the timing for a request as well as stashing the request-id header in the RequestLog. This filter
 * is useful when you <b>do not</b> want to use the {@link BardLoggingFilter}.
 */
@PreMatching
@Singleton
@Priority(1)
public class FiliInitializationFilter implements ContainerResponseFilter, ContainerRequestFilter {
    public static final String X_REQUEST_ID_HEADER = "x-request-id";
    private static final Pattern VALID_REQUEST_ID = Pattern.compile("[a-zA-Z0-9+/=-]+");


    @Override
    public void filter(ContainerRequestContext request) throws IOException {
        appendRequestId(request.getHeaders());
        RequestLogUtils.startTimingRequest();
    }

    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) throws IOException {
        RequestLogUtils.stopTimingRequest();
    }

    /**
     * Add a request id to pass to druid. Only added if x-request-id follows the format specified at:
     * https://devcenter.heroku.com/articles/http-request-id
     *
     * @param headers  Request id to add as queryId prefix to druid
     */
    public static void appendRequestId(MultivaluedMap<String, String> headers) {
        String requestId = headers.getFirst(X_REQUEST_ID_HEADER);
        if (isInvalidRequestId(requestId)) {
            return; // Ignore according to https://devcenter.heroku.com/articles/http-request-id
        }
        RequestLogUtils.addIdPrefix(requestId);
    }

    /**
     * Validate whether or not a string is acceptable as an x-request-id header argument.
     *
     * @param requestId  Request id to validate
     * @return True if the request id is not valid, false otherwise
     */
    public static boolean isInvalidRequestId(String requestId) {
        return StringUtils.isEmpty(requestId)
                || requestId.length() > 200
                || !VALID_REQUEST_ID.matcher(requestId).matches();
    }
}
