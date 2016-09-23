// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import com.yahoo.bard.webservice.logging.RequestLog;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

/**
 * Stub Servlet for testing servlet filters in isolation.
 */
@Path("test")
@Singleton
public class TestFilterServlet implements Runnable {

    List<Pair<AsyncResponse, RequestLog>> responses = new ArrayList<>();

    /**
     * Constructor.
     */
    @Inject
    public TestFilterServlet() {
        // No-Op
    }

    /**
     * Collect async responses in list then respond to all 1 second later.  This keeps Grizzly happy.
     *
     * @param uriInfo  Information about the URL for the request
     * @param asyncResponse  The response object to send the final response to
     */
    @GET
    @Timed(name = "testTimed")
    @Metered(name = "testMetered")
    @ExceptionMetered(name = "testExc")
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/data")
    public void getData(@Context UriInfo uriInfo, @Suspended AsyncResponse asyncResponse) {

        synchronized (responses) {
            if (responses.size() == 0) {
                // start release thread
                new Thread(this).start();
            }

            responses.add(new Pair<>(asyncResponse, RequestLog.dump()));
        }
    }

    /**
     * Mock for the data endpoint.
     *
     * @param uriInfo  Information about the URL for the request
     * @param asyncResponse  The response object to send the final response to
     */
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/data")
    public void postData(@Context UriInfo uriInfo, @Suspended AsyncResponse asyncResponse) {

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
        } catch (InterruptedException ignore) {
            // Do nothing
        }
    }

    /**
     * An endpoint at /fail.
     *
     * @param uriInfo  The URI info of the request
     * @param asyncResponse  The response to respond to
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/fail")
    public void getFail(@Context UriInfo uriInfo, @Suspended AsyncResponse asyncResponse) {

        // Process stream
        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignore) {
                    // Ignore
                }
                throw new IOException();
            }
        };

        // pass stream handler as response
        javax.ws.rs.core.Response rsp = javax.ws.rs.core.Response.ok(stream).build();
        asyncResponse.resume(rsp);
    }

    /**
     * A GET endpoint that takes a fancy-cased query parameter and returns it's value.
     *
     * @param param  Value to return
     *
     * @return the value at the query parameter
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/echoParam")
    public String getCaseInsensitiveQueryParam(@QueryParam("tEstingEChO") String param) {
        return param;
    }

    /**
     * A POST endpoint that takes a fancy-cased query parameter and does nothing.
     *
     * @param param  Value to return
     */
    @POST
    @Path("/echoParam")
    public void testSameCapitalizationDuplicate(@QueryParam("tEstingEChO") String param) {
        // Do nothing
    }

    /**
     * Bean to represent a pair.
     *
     * @param <F>  First type in the pair
     * @param <S>  Second type in the pair
     */
    static class Pair<F, S> {
        public final F first;
        public final S second;

        /**
         * Constructor.
         *
         * @param response  First element in the pair
         * @param context  Second element in the pair
         */
        Pair(F response, S context) {
            this.first = response;
            this.second = context;
        }
    }
}
