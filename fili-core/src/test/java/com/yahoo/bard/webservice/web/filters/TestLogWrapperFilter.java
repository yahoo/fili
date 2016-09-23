// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.logging.RequestLog;

import javax.inject.Singleton;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.client.ClientResponseContext;
import javax.ws.rs.client.ClientResponseFilter;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;

/**
 * Starts, stops and logs a wrapper timer and log line for a test.
 */
@PreMatching
@Singleton
public class TestLogWrapperFilter implements ContainerRequestFilter, ContainerResponseFilter, ClientRequestFilter,
       ClientResponseFilter {

    @Override
    public void filter(final ContainerRequestContext request) {
        RequestLog.startTiming("TestLogWrapper");
    }

    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) {
        RequestLog.stopTiming("TestLogWrapper");
        RequestLog.log();
    }

    @Override
    public void filter(final ClientRequestContext request) {
        RequestLog.startTiming("TestLogWrapper");
    }

    @Override
    public void filter(ClientRequestContext request, ClientResponseContext response) {
        RequestLog.stopTiming("TestLogWrapper");
        RequestLog.log();
    }
}
