// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import com.yahoo.bard.webservice.logging.RequestLog;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Stub Servlet for testing logging under the presence of exceptions.
 */
@Path("test")
@Singleton
public class TestLoggingServlet {
    /**
     * Constructor.
     */
    @Inject
    public TestLoggingServlet() {
        // No-Op
    }

    /**
     * Accept request and reply with OK.
     *
     * @param uriInfo  Information about the URL for the request
     * @param asyncResponse  The response object to send the final response to
     */
    @GET
    @Timed(name = "logTimed")
    @Metered(name = "logMetered")
    @ExceptionMetered(name = "logException")
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/log")
    public void getSucceed(@Context UriInfo uriInfo, @Suspended AsyncResponse asyncResponse) {
        RequestLog.startTiming(this);
        try {
            Thread.sleep(200);
        } catch (InterruptedException ignore) {
            // Do nothing
        }
        RequestLog.stopTiming(this);

        Response response = Response.status(Response.Status.OK).build();
        asyncResponse.resume(response);
    }

    /**
     * Accept request and reply with webapp exception and a simple string message.
     *
     * @param uriInfo  Information about the URL for the request
     */
    @GET
    @Timed(name = "logTimed")
    @Metered(name = "logMetered")
    @ExceptionMetered(name = "logException")
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/webbug")
    public void getFailWithWebAppException(@Context UriInfo uriInfo) {
        RequestLog.startTiming(this);
        try {
            Thread.sleep(200);
            throw new WebApplicationException(
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Oops! Web App Exception").build()
            );
        } catch (InterruptedException ignore) {
            // Do nothing
        }
    }

    /**
     * Accept request and reply with a generic runtime exception and a simple string message.
     *
     * @param uriInfo  Information about the URL for the request
     */
    @GET
    @Timed(name = "logTimed")
    @Metered(name = "logMetered")
    @ExceptionMetered(name = "logException")
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/genericbug")
    public void getFailWithRuntimeException(@Context UriInfo uriInfo) {
        RequestLog.startTiming(this);
        try {
            Thread.sleep(200);
            throw new RuntimeException("Oops! Generic Exception");
        } catch (InterruptedException ignore) {
            // Do nothing
        }
    }
}
