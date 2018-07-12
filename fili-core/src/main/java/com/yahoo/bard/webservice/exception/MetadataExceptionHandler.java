// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exception;

import com.yahoo.bard.webservice.web.apirequest.ApiRequest;

import java.util.Optional;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Like {@link DataExceptionHandler}, but handles the metadata endpoints.
 *
 * The data and metadata endpoints have very different response models (asynchronous vs. synchronous respectively). As
 * a result, there is not a clean way to unify them into a single exception handler. So they each
 * get their own
 */
public interface MetadataExceptionHandler {

    /**
     * Returns a Response for arbitrary Throwables.
     *
     * @param e  The throwable to generate a Response for
     * @param request  The bean containing the parsed request, may be empty if an exception was thrown
     * during request construction
     * @param uriInfo  The URI of the request
     * @param requestContext  HTTP request context for the request
     * @param metadataEntityName  The name of the metadata entity (dimension, metric, etc) being accessed
     *
     * @return The Response to send to the user
     */
    Response handleThrowable(
            Throwable e,
            Optional<ApiRequest> request,
            UriInfo uriInfo,
            ContainerRequestContext requestContext,
            String metadataEntityName
    );
}
