// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.ratelimit;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigException;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.bard.webservice.web.DataApiRequestTypeIdentifier;
import com.yahoo.bard.webservice.web.RateLimiter;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(DefaultRateLimiter.class);
    protected static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    protected static final RateLimitRequestToken REJECT_REQUEST_TOKEN =
            new CallbackRateLimitRequestToken(false, () -> { });
    protected static final RateLimitRequestToken BYPASS_TOKEN =
            new BypassRateLimitRequestToken();

    // Property names
    protected static final @NotNull String REQUEST_LIMIT_GLOBAL_KEY =
            SYSTEM_CONFIG.getPackageVariableName("request_limit_global");
    protected static final @NotNull String REQUEST_LIMIT_PER_USER_KEY =
            SYSTEM_CONFIG.getPackageVariableName("request_limit_per_user");
    protected static final @NotNull String REQUEST_LIMIT_UI_KEY =
            SYSTEM_CONFIG.getPackageVariableName("request_limit_ui");

    // Default values
    protected static final int DEFAULT_REQUEST_LIMIT_GLOBAL = 70;
    protected static final int DEFAULT_REQUEST_LIMIT_PER_USER = 2;
    protected static final int DEFAULT_REQUEST_LIMIT_UI = 52;

    protected static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();

    protected static final int DISABLED_RATE = -1;

    // Request limits
    protected final int requestLimitGlobal;
    protected final int requestLimitPerUser;
    protected final int requestLimitUi;

    // Live count holders
    protected final AtomicInteger globalCount = new AtomicInteger();
    protected final Map<String, AtomicInteger> userCounts = new ConcurrentHashMap<>();

    protected final Counter requestGlobalCounter;
    protected final Counter usersCounter;

    protected final Meter requestBypassMeter;
    protected final Meter requestUiMeter;
    protected final Meter requestUserMeter;
    protected final Meter rejectUiMeter;
    protected final Meter rejectUserMeter;

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
    protected AtomicInteger getCount(String userName) {
        AtomicInteger count = userCounts.get(userName);

        // Create a counter if we don't have one yet
        if (count == null) {
            userCounts.putIfAbsent(userName, new AtomicInteger());
            count = userCounts.get(userName);
            usersCounter.inc();
        }

        return count;
    }

    /**
     * Increment the initial count and check if the count has gone over the request limit.
     *
     * @param initialCount  Initial count that we're incrementing and checking against the limit
     * @param requestLimit  Limit to check the incremented initial count against
     *
     * @return True if the incremented count is less than or equal to the request limit, false if it's gone over and the
     * request limit isn't the DISABLED_RATE (-1).
     */
    protected boolean incrementAndCheckCount(AtomicInteger initialCount, int requestLimit) {
        int count = initialCount.incrementAndGet();
        if (count > requestLimit && requestLimit != DISABLED_RATE) {
            initialCount.decrementAndGet();
            LOG.info("reject: {} > {}", count, requestLimit);
            return false;
        }
        return true;
    }

    /**
     * Do the house keeping needed to reject the request.
     *
     * @param rejectMeter  Meter to count the rejection in
     * @param isRejectGlobal  Whether or not the rejection is on the global rate limit
     * @param isUIQuery  Whether or not the request is a UI Query
     * @param userName  Username of the user who made the request
     */
    protected void rejectRequest(Meter rejectMeter, boolean isRejectGlobal, boolean isUIQuery, String userName) {
        rejectMeter.mark();
        String limitType = isRejectGlobal ? "GLOBAL" : isUIQuery ? "UI" : "USER";
        LOG.info("{} limit {}", limitType, userName);
    }

    @Override
    public RateLimitRequestToken getToken(ContainerRequestContext request) {
        MultivaluedMap<String, String> headers = Utils.headersToLowerCase(request.getHeaders());

        if (
            DataApiRequestTypeIdentifier.isBypass(headers) ||
            DataApiRequestTypeIdentifier.isCorsPreflight(request.getMethod(), request.getSecurityContext())
        ) {
            // Bypass and CORS Preflight requests are unlimited
            requestBypassMeter.mark();
            return BYPASS_TOKEN;
        }
        SecurityContext securityContext = request.getSecurityContext();
        Principal user = securityContext == null ? null : securityContext.getUserPrincipal();
        String userName = String.valueOf(user == null ? null : user.getName());

        boolean isUIQuery = DataApiRequestTypeIdentifier.isUi(headers);
        Meter requestMeter;
        Meter rejectMeter;
        int requestLimit;

        if (isUIQuery) {
            requestMeter = requestUiMeter;
            rejectMeter = rejectUiMeter;
            requestLimit = requestLimitUi;
        } else {
            requestMeter = requestUserMeter;
            rejectMeter = rejectUserMeter;
            requestLimit = requestLimitPerUser;
        }

        AtomicInteger count = getCount(userName);
        return createNewRateLimitRequestToken(count, userName, isUIQuery, requestLimit, requestMeter, rejectMeter);
    }

    /**
     * Creates a new RateLimitRequestToken.
     *
     * @param count  The atomic reference that holds the amount of in-flight requests the user owns
     * @param userName  The user who launched the request
     * @param isUIQuery  Whether or not this query was generated from the UI
     * @param requestLimit  The limit of requests the user is allowed to launch
     * @param requestMeter  Meter tracking the amount of requests that have been launched
     * @param rejectMeter  Meter tracking the amount of requests that have been rejected
     *
     * @return a new RateLimitRequestToken, representing an in-flight (or rejected) request that is tracked by the
     * RateLimiter
     */
    protected RateLimitRequestToken createNewRateLimitRequestToken(AtomicInteger count, String userName,
            boolean isUIQuery, int requestLimit, Meter requestMeter, Meter rejectMeter) {
        if (!incrementAndCheckCount(globalCount, requestLimitGlobal)) {
            rejectRequest(rejectMeter, true, isUIQuery, userName);
            return REJECT_REQUEST_TOKEN;
        }


        // Bind to the user
        if (!incrementAndCheckCount(count, requestLimit)) {
            // Decrement the global count that had already been incremented
            globalCount.decrementAndGet();
            rejectRequest(rejectMeter, false, isUIQuery, userName);
            return REJECT_REQUEST_TOKEN;
        }

        // Measure the accepted request and current open connections
        requestMeter.mark();
        requestGlobalCounter.inc();

        // Return new request token
        RateLimitCleanupOnRequestComplete callback = generateCleanupClosure(count, userName);
        return new CallbackRateLimitRequestToken(true, callback);
    }

    /**
     * Creates a callback to be passed to a token to execute when a request has completed. The callback handles
     * decrementing the global and user counters.
     *
     * @param count  The AtomicInteger that stores the amount of in-flight requests an individual user owns
     * @param userName  The name of the user that made the request
     *
     * @return A callback implementation to be given to a CallbackRateLimitRequestToken
     */
    protected RateLimitCleanupOnRequestComplete generateCleanupClosure(AtomicInteger count, String userName) {
        return () -> {
            if (globalCount.decrementAndGet() < 0) {
                // Reset to 0 if it falls below 0
                int old = globalCount.getAndSet(0);
                LOG.error("Lost global count {} on user {}", old, userName);
            }
            if (count.decrementAndGet() < 0) {
                // Reset to 0 if it falls below 0
                int old = count.getAndSet(0);
                LOG.error("Lost user count {} on user {}", old, userName);
                throw new IllegalStateException("Lost user count");
            }
        };
    }
}
