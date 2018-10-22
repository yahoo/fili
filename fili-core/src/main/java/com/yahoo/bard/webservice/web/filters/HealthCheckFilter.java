// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.application.HealthCheckRegistryFactory;
import com.yahoo.bard.webservice.logging.RequestLog;

import com.codahale.metrics.health.HealthCheck.Result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
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

        RequestLog.startTiming(this);
        String path = requestContext.getUriInfo().getAbsolutePath().getPath();

        if (path.startsWith("/v1/data") || path.startsWith("/data")) {
            // See if we have any unhealthy checks
            Map<String, Result> unhealthyChecks = getUnhealthy();
            if (!unhealthyChecks.keySet().isEmpty()) {
                StringBuilder debugMsgBuilder = new StringBuilder();
                if (requestContext.getSecurityContext().getUserPrincipal() != null) {
                    String user = requestContext.getSecurityContext().getUserPrincipal().getName();
                    debugMsgBuilder.append("User=").append(user).append("\n");
                }
                debugMsgBuilder.append(renderUri(requestContext.getUriInfo().getRequestUri())).append("\n");

                debugMsgBuilder.append("Timestamp: ");
                debugMsgBuilder.append(java.time.Clock.systemUTC().instant().toString());
                debugMsgBuilder.append("\n");

                unhealthyChecks.entrySet()
                        .forEach(entry -> {
                            LOG.error("Healthcheck '{}' failed: {}", entry.getKey(), entry.getValue());
                            LOG.error(debugMsgBuilder.toString());
                        });
                RequestLog.stopTiming(this);
                debugMsgBuilder.insert(0, "Service is unhealthy. At least 1 healthcheck is failing\n");
                requestContext.abortWith(
                        Response.status(Status.SERVICE_UNAVAILABLE)
                                .entity(debugMsgBuilder)
                                .build()
                );
                return;
            }
        }
        RequestLog.stopTiming(this);
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

    /**
     * Render the URI as a string.
     *
     * @param uri The URI to render
     *
     * @return A String representation of the URI
     */
    protected String renderUri(URI uri) {
        return uri.toASCIIString();
    }
}
