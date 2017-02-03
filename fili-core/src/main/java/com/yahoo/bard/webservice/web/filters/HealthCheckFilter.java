// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.application.HealthCheckRegistryFactory;
import com.yahoo.bard.webservice.logging.RequestLog;

import com.codahale.metrics.health.HealthCheck.Result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Priority;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * Filter all /data requests to respond SERVICE_UNAVAILABLE if any health check fails.
 */
@Singleton
@Priority(6)
public class HealthCheckFilter implements ContainerRequestFilter {

    private static final Logger LOG = LoggerFactory.getLogger(HealthCheckFilter.class);

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {

        RequestLog.startTiming(this);
        String path = requestContext.getUriInfo().getAbsolutePath().getPath();

        if (path.startsWith("/v1/data") || path.startsWith("/data")) {
            // See if we have any unhealthy checks
            Optional<Map.Entry<String, Result>> firstUnhealthy = getFirstUnhealthy();

            // If we do, stop the request
            firstUnhealthy.ifPresent(entry -> {
                RequestLog.stopTiming(this);
                LOG.error("Healthcheck '{}' failed: {}", entry.getKey(), entry.getValue());
                requestContext.abortWith(
                        Response.status(Status.SERVICE_UNAVAILABLE)
                                .entity("Service is unhealthy. At least 1 healthcheck is failing")
                                .build()
                );
            });

            // If we did, return since we already stopped the timer
            if (firstUnhealthy.isPresent()) {
                return;
            }
        }
        RequestLog.stopTiming(this);
    }

    /**
     * Get the first healthcheck that is unhealthy.
     *
     * @return the first healthcheck that is found to be unhealthy, if any.
     */
    public Optional<Map.Entry<String, Result>> getFirstUnhealthy() {
        return HealthCheckRegistryFactory.getRegistry().runHealthChecks().entrySet().stream()
                .filter(entry -> !entry.getValue().isHealthy())
                .findFirst();
    }
}
