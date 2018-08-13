// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exception;

import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.apirequest.ApiRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;

/**
 * Default handler of Slices servlet errors.
 */
public class FiliSlicesExceptionHandler implements MetadataExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(FiliSlicesExceptionHandler.class);

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
        } else if (e instanceof IOException) {
            String msg = String.format("Internal server error. IOException : %s", e.getMessage());
            LOG.error(msg, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(msg).build();
        } else {
            String msg = ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format(e.getMessage());
            LOG.info(msg, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }
}
