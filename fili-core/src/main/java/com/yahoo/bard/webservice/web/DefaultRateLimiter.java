// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigException;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.util.Utils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import java.security.Principal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.SecurityContext;

/**
 * This is the default implementation of a rate limiter.
 */
public class DefaultRateLimiter implements RateLimiter {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    // Property names
    private static final @NotNull String REQUEST_LIMIT_GLOBAL_KEY =
            SYSTEM_CONFIG.getPackageVariableName("request_limit_global");
    private static final @NotNull String REQUEST_LIMIT_PER_USER_KEY =
            SYSTEM_CONFIG.getPackageVariableName("request_limit_per_user");
    private static final @NotNull String REQUEST_LIMIT_UI_KEY =
            SYSTEM_CONFIG.getPackageVariableName("request_limit_ui");

    // Default values
    private static final int DEFAULT_REQUEST_LIMIT_GLOBAL = 70;
    private static final int DEFAULT_REQUEST_LIMIT_PER_USER = 2;
    private static final int DEFAULT_REQUEST_LIMIT_UI = 52;

    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();

    // Request limits
    private final int requestLimitGlobal;
    private final int requestLimitPerUser;
    private final int requestLimitUi;

    // Live count holders
    private final AtomicInteger globalCount = new AtomicInteger();
    private final Map<String, AtomicInteger> userCounts = new ConcurrentHashMap<>();

    private final Counter requestGlobalCounter;
    private final Counter usersCounter;

    private final Meter requestBypassMeter;
    private final Meter requestUiMeter;
    private final Meter requestUserMeter;
    private final Meter rejectUiMeter;
    private final Meter rejectUserMeter;

    /**
     * Loads defaults and creates DefaultRateLimiter.
     *
     * @throws SystemConfigException If any parameters fail to load
     */
    public DefaultRateLimiter() throws SystemConfigException {
        // Load limits
        requestLimitGlobal = SYSTEM_CONFIG.getIntProperty(REQUEST_LIMIT_GLOBAL_KEY, DEFAULT_REQUEST_LIMIT_GLOBAL);
        requestLimitPerUser = SYSTEM_CONFIG.getIntProperty(REQUEST_LIMIT_PER_USER_KEY, DEFAULT_REQUEST_LIMIT_PER_USER);
        requestLimitUi = SYSTEM_CONFIG.getIntProperty(REQUEST_LIMIT_UI_KEY, DEFAULT_REQUEST_LIMIT_UI);

        // Register counters for currently active requests
        usersCounter = REGISTRY.counter("ratelimit.count.users");
        requestGlobalCounter = REGISTRY.counter("ratelimit.count.global");

        // Register meters for number of requests
        requestUserMeter = REGISTRY.meter("ratelimit.meter.request.user");
        requestUiMeter = REGISTRY.meter("ratelimit.meter.request.ui");
        requestBypassMeter = REGISTRY.meter("ratelimit.meter.request.bypass");
        rejectUserMeter = REGISTRY.meter("ratelimit.meter.reject.user");
        rejectUiMeter = REGISTRY.meter("ratelimit.meter.reject.ui");
    }

    /**
     * Get the current count for this username. If user does not have a counter, create one.
     *
     * @param userName  Username to get the count for
     *
     * @return The atomic count for the user
     */
    private AtomicInteger getCount(String userName) {
        AtomicInteger count = userCounts.get(userName);

        // Create a counter if we don't have one yet
        if (count == null) {
            userCounts.putIfAbsent(userName, new AtomicInteger());
            count = userCounts.get(userName);
            usersCounter.inc();
        }

        return count;
    }

    @Override
    public RateLimitRequestToken getToken(ContainerRequestContext request) {
        MultivaluedMap<String, String> headers = Utils.headersToLowerCase(request.getHeaders());
        SecurityContext securityContext = request.getSecurityContext();
        Principal user = securityContext == null ? null : securityContext.getUserPrincipal();
        String userName = String.valueOf(user == null ? null : user.getName());

        if (DataApiRequestTypeIdentifier.isBypass(headers) ||
                DataApiRequestTypeIdentifier.isCorsPreflight(request.getMethod(), request.getSecurityContext())) {
            // Bypass and CORS Preflight requests are unlimited
            return new BypassRateLimitRequestToken(requestBypassMeter);
        } else {
            // Is either a UI query or a User query
            boolean isUIQuery = DataApiRequestTypeIdentifier.isUi(headers);
            return new OutstandingRateLimitedRequestToken(
                    user,
                    isUIQuery ? requestLimitUi : requestLimitPerUser,
                    requestLimitGlobal,
                    getCount(userName),
                    globalCount,
                    isUIQuery ? requestUiMeter : requestUserMeter,
                    isUIQuery ? rejectUiMeter : rejectUserMeter,
                    requestGlobalCounter
            );
        }
    }
}
