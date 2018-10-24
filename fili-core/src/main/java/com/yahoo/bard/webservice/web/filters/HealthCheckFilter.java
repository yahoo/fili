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
            StringBuilder debugMsgBuilder = builderErrorResponseBody(requestContext);
            if (!unhealthyChecks.keySet().isEmpty()) {
                unhealthyChecks.forEach((key, value) -> LOG.error("Healthcheck '{}' failed: {}", key, value));

                RequestLog.stopTiming(this);
                debugMsgBuilder.insert(0, "Service is unhealthy. At least 1 healthcheck is failing\n");
                requestContext.abortWith(
                        Response.status(Status.SERVICE_UNAVAILABLE)
                                .entity(debugMsgBuilder.toString())
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
     * Gathers some interesting data from the request and builds a string out of it.
     *
     * @param requestContext The request context. Contains info we want to log
     * @return the StringBuilder containing our logging info
     */
    protected StringBuilder builderErrorResponseBody(ContainerRequestContext requestContext) {
        StringBuilder debugMsgBuilder = new StringBuilder();
        if (requestContext.getSecurityContext().getUserPrincipal() != null) {
            String user = requestContext.getSecurityContext().getUserPrincipal().getName();
            debugMsgBuilder.append("User=")
                    .append(user)
                    .append(System.lineSeparator());
        }
        debugMsgBuilder.append("Timestamp: ")
                .append(java.time.Clock.systemUTC().instant().toString())
                .append(System.lineSeparator());

        debugMsgBuilder.append("Request ID: ")
                .append(RequestLog.getId())
                .append(System.lineSeparator());

        return debugMsgBuilder;
    }
}
