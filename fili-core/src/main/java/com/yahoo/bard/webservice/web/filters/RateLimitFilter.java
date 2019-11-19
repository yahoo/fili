// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import static com.yahoo.bard.webservice.web.ResponseCode.RATE_LIMIT;

import com.yahoo.bard.webservice.config.SystemConfigException;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.web.RateLimiter;
import com.yahoo.bard.webservice.web.ratelimit.DefaultRateLimiter;
import com.yahoo.bard.webservice.web.ratelimit.RateLimitRequestToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Response;

/**
 * Filter all requests to respond RATE_LIMIT if user/global limits are exceeded.
 */
@PreMatching
@Singleton
@Priority(5)
public class RateLimitFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static final Logger LOG = LoggerFactory.getLogger(RateLimitFilter.class);
    private static final String PROPERTY_TOKEN = RateLimiter.class.getName() + ".token";
    private static final String DATA_PATH = "/v1/data";

    protected final RateLimiter rateLimiter;

    /**
     * Constructs a RateLimitFilter using the provided rate limiter.
     *
     * @param rateLimiter  RateLimiter object to handle rate limiting logic.
     *
     * @throws SystemConfigException if RateLimiter construction fails.
     */
    @Inject
    public RateLimitFilter(RateLimiter rateLimiter) throws SystemConfigException {
        this.rateLimiter = rateLimiter;
    }

    /**
     * Load DefaultRateLimiter for this filter.
     *
     * @throws SystemConfigException  If any critical configuration fails to load for the DefaultRateLimiter
     *
     * @deprecated Use the constructor that takes a RateLimiter
     */
    @Deprecated
    public RateLimitFilter() throws SystemConfigException {
        rateLimiter = new DefaultRateLimiter();
    }

    @Override
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public void filter(ContainerRequestContext request) throws IOException {

        RequestLog.startTiming(this);
        URI uri = request.getUriInfo().getAbsolutePath();
        String path = uri.getPath();

        // Determine if we should filter based on URL path
        if (path.startsWith(DATA_PATH) || path.startsWith("/test")) {
            RateLimitRequestToken token = rateLimiter.getToken(request);

            // Add the token to the request if it was bound
            if (token.isBound()) {
                request.setProperty(PROPERTY_TOKEN, token);
            } else {
                String msg = String.format("Rate limit reached. Reject %s", uri.toString());
                LOG.debug(msg);
                RequestLog.stopTiming(this);
                request.abortWith(Response.status(RATE_LIMIT).entity(msg).build());
                return;
            }
        }

        RequestLog.stopTiming(this);
    }

    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) throws IOException {
        // Release the token
        RateLimitRequestToken token = (RateLimitRequestToken) request.getProperty(PROPERTY_TOKEN);
        if (token != null) {
            token.close();
        }
    }
}
