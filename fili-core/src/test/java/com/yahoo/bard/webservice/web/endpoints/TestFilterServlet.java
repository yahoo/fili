// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import com.yahoo.bard.webservice.logging.RequestLog;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

/**
 * Stub Servlet for testing servlet filters in isolation
 */
@Path("test")
@Singleton
public class TestFilterServlet implements Runnable {
    @Inject
    public TestFilterServlet() {
        // No-Op
    }

    static class Pair<F, S> {
        public final F first;
        public final S second;

        Pair(F response, S context) {
            this.first = response;
            this.second = context;
        }
    }

    ArrayList<Pair<AsyncResponse, RequestLog>> responses = new ArrayList<>();

    /**
     * Collect async responses in list then respond to all 1 second later.  This keeps Grizzly happy.
     *
     * @param uriInfo
     * @param asyncResponse
     */
    @GET
    @Timed(name = "testTimed")
    @Metered(name = "testMetered")
    @ExceptionMetered(name = "testExc")
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/data")
    public void getData(
        @Context final UriInfo uriInfo,
        @Suspended final AsyncResponse asyncResponse
        ) {

        synchronized (responses) {

            if (responses.size() == 0) {
                // start release thread
                new Thread(this).start();
            }

            responses.add(new Pair<AsyncResponse, RequestLog>(asyncResponse, RequestLog.dump()));
        }
    }

    /**
     * Mock for the data endpoint
     *
     * @param uriInfo
     * @param asyncResponse
     */
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/data")
    public void postData(
        @Context final UriInfo uriInfo,
        @Suspended final AsyncResponse asyncResponse
        ) {

        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 65536; i++) {
            b.append((char) ('A' + i % 26));
        }

        asyncResponse.resume(b.toString());
    }

    @Override
    public void run() {
        try {
            Thread.sleep(1000);

            // release waiting requests
            synchronized (responses) {
                for (Pair<AsyncResponse, RequestLog> response : responses) {
                    RequestLog.restore(response.second);
                    response.first.resume("OK");
                }

                responses.clear();
            }
        } catch (InterruptedException e) {
            // Do nothing
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/fail")
    public void getFail(
        @Context final UriInfo uriInfo,
        @Suspended final AsyncResponse asyncResponse
        ) {

        // Process stream
        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // Ignore
                }
                throw new IOException();
            }
        };

        // pass stream handler as response
        javax.ws.rs.core.Response rsp = javax.ws.rs.core.Response.ok(stream).build();
        asyncResponse.resume(rsp);
    }
}
