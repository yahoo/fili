// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exception;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.apirequest.ApiRequest;
import com.yahoo.bard.webservice.web.handlers.RequestHandlerUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;

/**
 * Handles exceptions for the JobsServlet.
 */
public class FiliJobsExceptionHandler implements MetadataExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(FiliJobsExceptionHandler.class);

    private final ObjectMappersSuite mappers;

    /**
     * Handler for dealing with exceptions in the Jobs servlet.
     *
     * @param objectMappers  A suite of JSON parsing tools
     */
    @Inject
    public FiliJobsExceptionHandler(ObjectMappersSuite objectMappers) {
        this.mappers = objectMappers;
    }

    @Override
    public Response handleThrowable(
            Throwable e,
            Optional<? extends ApiRequest> request,
            ContainerRequestContext requestContext
    ) {
        if (e instanceof RequestValidationException) {
            LOG.debug(e.getMessage(), e);
            RequestValidationException rve = (RequestValidationException) e;
            return RequestHandlerUtils.makeErrorResponse(rve.getStatus(), rve, mappers.getMapper().writer());
        }
        String msg = ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format(e.getMessage());
        LOG.info(msg, e);
        return Response.status(INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
    }
}
