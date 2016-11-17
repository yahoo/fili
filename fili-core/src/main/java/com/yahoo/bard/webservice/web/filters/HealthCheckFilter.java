// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.application.HealthCheckRegistryFactory;
import com.yahoo.bard.webservice.logging.RequestLogUtils;

import com.codahale.metrics.health.HealthCheck.Result;
import com.codahale.metrics.health.HealthCheckRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.SortedMap;

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
        URI uri = requestContext.getUriInfo().getAbsolutePath();
        String path = uri.getPath();

        if (path.startsWith("/v1/data") || path.startsWith("/data")) {
            if (!isHealthy()) {
                String msg = String.format("reject %s", uri.toString());
                LOG.error(msg);
                RequestLogUtils.stopTiming(this);
                requestContext.abortWith(Response.status(Status.SERVICE_UNAVAILABLE).entity(msg).build());
                return;
            }
        }
        RequestLogUtils.stopTiming(this);
    }

    /**
     * Run through all registered health checks to make sure all are healthy.
     *
     * @return true only if all health checks pass
     */
    public boolean isHealthy() {
        HealthCheckRegistry registry = HealthCheckRegistryFactory.getRegistry();

        SortedMap<String, Result> checks = registry.runHealthChecks();

        if (checks.size() == 0) {
            LOG.error("No healthchecks registered.");
            return false;
        }

        // Loop over all registered health checks for any not healthy
        for (Map.Entry<String, Result> checkEntry : checks.entrySet()) {
            LOG.trace(String.format("Checking %s...", checkEntry.getKey()));
            if (!checkEntry.getValue().isHealthy()) {
                LOG.error(String.format("%s check failed: %s", checkEntry.getKey(), checkEntry.getValue().toString()));
                return false;
            }
        }

        return true;
    }
}
