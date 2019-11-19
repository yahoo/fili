// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exception;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.GATEWAY_TIMEOUT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

import com.yahoo.bard.webservice.data.dimension.TimeoutException;
import com.yahoo.bard.webservice.table.resolver.NoMatchFoundException;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.handlers.RequestHandlerUtils;

import com.fasterxml.jackson.databind.ObjectWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * The default implementation of {@link DataExceptionHandler}.
 * <p>
 * This class handles the following cases:
 * <ol>
 * <li> RequestValidationException - builds an error response and returns the HTTP status stored in the
 *  exception
 * <li> NoMatchFoundException - Returns a 500 (Internal Server Error)
 * <li> TimeoutException - Returns a 504 (Gateway Timeout)
 * <li> Error | Exception - A 400 with an response payload containing the throwable's message
 * </ol>
 */
public class FiliDataExceptionHandler implements DataExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(FiliDataExceptionHandler.class);

    @Override
    public void handleThrowable(
            Throwable e,
            AsyncResponse asyncResponse,
            Optional<DataApiRequest> apiRequest,
            ContainerRequestContext containerRequestContext,
            ObjectWriter writer
    ) {
        if (e instanceof RequestValidationException) {
            LOG.debug(e.getMessage(), e);
            RequestValidationException rve = (RequestValidationException) e;
            asyncResponse.resume(RequestHandlerUtils.makeErrorResponse(rve.getStatus(), rve, writer));
        } else if (e instanceof NoMatchFoundException) {
            LOG.info(
                    "Exception processing request: {}.  Request: {}",
                    e.getMessage(),
                    containerRequestContext.getUriInfo().getRequestUri().toString()
            );
            asyncResponse.resume(RequestHandlerUtils.makeErrorResponse(INTERNAL_SERVER_ERROR, e, writer));
        } else if (e instanceof TimeoutException) {
            LOG.info(
                    "Exception processing request: {}.  Request: {}",
                    e.getMessage(),
                    containerRequestContext.getUriInfo().getRequestUri().toString()
            );
            asyncResponse.resume(RequestHandlerUtils.makeErrorResponse(GATEWAY_TIMEOUT, e, writer));
        } else {
            LOG.info(
                    "Exception processing request: {}.  Request: {}",
                    e.getMessage(),
                    containerRequestContext.getUriInfo().getRequestUri().toString()
            );
            asyncResponse.resume(RequestHandlerUtils.makeErrorResponse(BAD_REQUEST, e, writer));
        }
    }
}
