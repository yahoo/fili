// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import static com.yahoo.bard.webservice.web.ResponseCode.RATE_LIMIT;

import com.yahoo.bard.webservice.config.SystemConfigException;
import com.yahoo.bard.webservice.logging.RequestLogUtils;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.bard.webservice.web.DataApiRequestTypeIdentifier;
import com.yahoo.bard.webservice.web.RateLimiter;
import com.yahoo.bard.webservice.web.RateLimiter.RequestToken;
import com.yahoo.bard.webservice.web.RateLimiter.RequestType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.security.Principal;

import javax.annotation.Priority;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

/**
 * Filter all requests to respond RATE_LIMIT if user/global limits are exceeded.
 */
@PreMatching
@Singleton
@Priority(5)
public class RateLimitFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static final String PROPERTY_TOKEN = RateLimiter.class.getName() + ".token";
    private static final Logger LOG = LoggerFactory.getLogger(RateLimitFilter.class);

    protected final RateLimiter rateLimiter;

    /**
     * Load RateLimiter for this filter.
     *
     * @throws SystemConfigException  If any critical configuration fails to load for the RateLimiter
     */
    public RateLimitFilter() throws SystemConfigException {
        rateLimiter = new RateLimiter();
    }

    @Override
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public void filter(ContainerRequestContext request) throws IOException {

        RequestLogUtils.startTiming(this);
        URI uri = request.getUriInfo().getAbsolutePath();
        String path = uri.getPath();

        // Determine if we should filter based on URL path
        // TODO: Make this based on resource mapped to or at least more configurable with _not_ strings
        if (path.startsWith("/v1/data") || path.startsWith("/data") || path.startsWith("/test")) {

            MultivaluedMap<String, String> headers = Utils.headersToLowerCase(request.getHeaders());

            // Pick RequestType
            RequestType type;
            if (DataApiRequestTypeIdentifier.isBypass(headers)) {
                // Bypass requests are unlimited
                type = RequestType.BYPASS;
            } else if (
                    DataApiRequestTypeIdentifier.isCorsPreflight(request.getMethod(), request.getSecurityContext())
            ) {
                // CORS Preflight requests are unlimited
                type = RequestType.BYPASS;
            } else if (DataApiRequestTypeIdentifier.isUi(headers)) {
                // UI requests have a different limit
                type = RequestType.UI;
            } else {
                type = RequestType.USER;
            }

            // Get the token
            SecurityContext securityContext = request.getSecurityContext();
            Principal user = securityContext == null ? null : securityContext.getUserPrincipal();
            RequestToken token = rateLimiter.getToken(type, user);

            // Add the token to the request if it was bound
            if (token.isBound()) {
                request.setProperty(PROPERTY_TOKEN, token);
            } else {
                String msg = String.format("Rate limit reached. Reject %s", uri.toString());
                LOG.debug(msg);
                RequestLogUtils.stopTiming(this);
                request.abortWith(Response.status(RATE_LIMIT).entity(msg).build());
                return;
            }
        }

        RequestLogUtils.stopTiming(this);
    }

    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) throws IOException {
        // Release the token
        RequestToken token = (RequestToken) request.getProperty(PROPERTY_TOKEN);
        if (token != null) {
            token.close();
        }
    }
}
