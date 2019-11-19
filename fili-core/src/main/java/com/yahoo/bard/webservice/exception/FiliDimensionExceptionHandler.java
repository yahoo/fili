// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exception;

import static com.yahoo.bard.webservice.web.ResponseCode.INSUFFICIENT_STORAGE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.RowLimitReachedException;
import com.yahoo.bard.webservice.web.apirequest.ApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DimensionsApiRequest;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * Default implementation of the MetadataExceptionHandler for the DimensionServlet.
 */
public class FiliDimensionExceptionHandler implements MetadataExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(FiliDimensionExceptionHandler.class);

    @Override
    public Response handleThrowable(
        Throwable e,
        Optional<? extends ApiRequest> request,
        ContainerRequestContext requestContext
    ) {
        if (e instanceof RequestValidationException) {
            LOG.debug(e.getMessage(), e);
            RequestValidationException rve = (RequestValidationException) e;
            return Response.status(rve.getStatus()).entity(rve.getErrorHttpMsg()).build();
        } else if (e instanceof RowLimitReachedException) {
            DimensionsApiRequest dimensionRequest = (DimensionsApiRequest) request.get();
            String msg = String.format(
                "Row limit exceeded for dimension %s: %s",
                dimensionRequest.getDimension(),
                e.getMessage()
            );
            LOG.debug(msg, e);
            return Response.status(INSUFFICIENT_STORAGE).entity(msg).build();
        } else if (e instanceof JsonProcessingException) {
            String msg = ErrorMessageFormat.INTERNAL_SERVER_ERROR_ON_JSON_PROCESSING.format(e.getMessage());
            LOG.error(msg, e);
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity(msg).build();
        } else {
            String msg = ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format(e.getMessage());
            LOG.debug(msg, e);
            return Response.status(BAD_REQUEST).entity(msg).build();
        }
    }
}
