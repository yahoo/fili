// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.application.HealthCheckRegistryFactory;
import com.yahoo.bard.webservice.logging.RequestLogUtils;

import com.codahale.metrics.health.HealthCheck.Result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

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
        RequestLogUtils.startTiming(this);
        String path = requestContext.getUriInfo().getAbsolutePath().getPath();

        if (path.startsWith("/v1/data") || path.startsWith("/data")) {
            // See if we have any unhealthy checks
            Map<String, Result> unhealthyChecks = getUnhealthy();
            if (!unhealthyChecks.keySet().isEmpty()) {
                unhealthyChecks.entrySet()
                        .forEach(entry -> LOG.error("Healthcheck '{}' failed: {}", entry.getKey(), entry.getValue()));
                RequestLogUtils.stopTiming(this);
                requestContext.abortWith(
                        Response.status(Status.SERVICE_UNAVAILABLE)
                                .entity("Service is unhealthy. At least 1 healthcheck is failing")
                                .build()
                );
                return;
            }
        }
        RequestLogUtils.stopTiming(this);
    }

    /**
     * Get all the failing healthchecks.
     *
     * @return A map of all the health checks that are unhealthy
     */
    public Map<String, Result> getUnhealthy() {
        return HealthCheckRegistryFactory.getRegistry().runHealthChecks().entrySet().stream()
                .filter(entry -> !entry.getValue().isHealthy())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
