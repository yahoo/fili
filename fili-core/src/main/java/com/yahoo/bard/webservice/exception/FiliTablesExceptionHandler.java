// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exception;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.apirequest.ApiRequest;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;

/**
 * Handles exceptions in the TablesEndpoint.
 */
public class FiliTablesExceptionHandler implements MetadataExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(FiliTablesExceptionHandler.class);

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
        } else if (e instanceof JsonProcessingException) {
            String msg = ErrorMessageFormat.INTERNAL_SERVER_ERROR_ON_JSON_PROCESSING.format(e.getMessage());
            LOG.error(msg, e);
            return Response.status(INTERNAL_SERVER_ERROR).entity(msg).build();
        } else {
            String msg = ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format(e.getMessage());
            LOG.info(msg, e);
            return Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
        }
    }
}
