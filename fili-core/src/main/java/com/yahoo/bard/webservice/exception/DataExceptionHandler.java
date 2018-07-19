// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exception;

import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;

import com.fasterxml.jackson.databind.ObjectWriter;

import java.util.Optional;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * Allows a customer to inject custom code for handling Exceptions in Fili's DataServlet.
 * <p>
 * For example, customers may want to return 500's for some custom exceptions, or return
 * an empty set when dimension rows are not found.
 */
public interface DataExceptionHandler {

    /**
     * Handles any exception generated during response processing.
     *
     * @param e  The generated throwable (i.e. exception or error)
     * @param asyncResponse  A response that can be used to respond asynchronously
     * (probably with some sort of appropriate error code)
     * @param apiRequest  A bean containing parsed information about the request, note that
     * this won't exist if an exception was thrown while constructing the request
     * @param containerRequestContext  HTTP request context
     * @param writer  A tool for serializing JSON
     */
    void handleThrowable(
        Throwable e,
        AsyncResponse asyncResponse,
        Optional<DataApiRequest> apiRequest,
        ContainerRequestContext containerRequestContext,
        ObjectWriter writer
    );
}
