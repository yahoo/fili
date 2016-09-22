// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.exceptionmappers;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * An exception mapper that guarantees that an uncaught exception at this point is caught and an appropriate response is
 * forwarded to the appropriate filters in the Jersey stack.
 */
@Provider
public class LogExceptionMapper implements ExceptionMapper<Throwable> {

    @Override
    @Produces(MediaType.APPLICATION_JSON)
    public Response toResponse(Throwable t) {
        // If the exception is of type WebApplicationException it already has a Response object
        // If not, we construct one with the appropriate status and message
        if (t instanceof WebApplicationException) {
            return ((WebApplicationException) t).getResponse();
        } else {
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity(t.getMessage()).build();
        }
    }
}
