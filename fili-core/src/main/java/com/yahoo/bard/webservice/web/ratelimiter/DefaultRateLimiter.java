// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.ratelimiter;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigException;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.bard.webservice.web.DataApiRequestTypeIdentifier;
import com.yahoo.bard.webservice.web.RateLimitRequestToken;
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

    private static final int DISABLED_RATE = -1;

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

    /**
     * Increment the initial count and check if the count has gone over the request limit.
     *
     * @param initialCount  Initial count that we're incrementing and checking against the limit
     * @param requestLimit  Limit to check the incremented initial count against
     *
     * @return True if the incremented count is <= the request limit, false if it's gone over and the request limit
     * isn't the DISABLED_RATE (-1).
     */
    private boolean incrementAndCheckCount(AtomicInteger initialCount, int requestLimit) {
        int count = initialCount.incrementAndGet();
        if (count > requestLimit && requestLimit != DISABLED_RATE) {
            initialCount.decrementAndGet();
            LOG.info("reject: {} > {}", count, requestLimit);
            return false;
        }
        return true;
    }

    @Override
    public RateLimitRequestToken getToken(ContainerRequestContext request) {
        MultivaluedMap<String, String> headers = Utils.headersToLowerCase(request.getHeaders());
        SecurityContext securityContext = request.getSecurityContext();
        Principal user = securityContext == null ? null : securityContext.getUserPrincipal();

        if (DataApiRequestTypeIdentifier.isBypass(headers) ||
                DataApiRequestTypeIdentifier.isCorsPreflight(request.getMethod(), request.getSecurityContext())) {
            // Bypass and CORS Preflight requests are unlimited
            return new BypassRateLimitRequestToken(requestBypassMeter);
        } else {
            // Is either a UI query or a User query
            return new OutstandingRateLimitRequestToken(user, DataApiRequestTypeIdentifier.isUi(headers));
        }
    }

    /**
     * Simple rate limiting token implementation.
     */
    private class OutstandingRateLimitRequestToken implements RateLimitRequestToken {
        private boolean isBound;
        private String userName;
        private AtomicInteger count;

        private Meter requestMeter;
        private Meter rejectMeter;
        /**
         * Create a token, and decide whether to accept and reject the request.
         *
         * @param user  The user the request belongs to
         * @param isUIQuery  A boolean representing whether the request sent from the UI or from some other source
         */
        OutstandingRateLimitRequestToken(Principal user, Boolean isUIQuery) {
            this.userName = String.valueOf(user == null ? null : user.getName());
            this.count = getCount(userName);

            requestMeter = isUIQuery ? requestUiMeter : requestUserMeter;
            rejectMeter = isUIQuery ? rejectUiMeter : rejectUserMeter;
            int requestLimit = isUIQuery ? requestLimitUi : requestLimitPerUser;

            // Bind globally
            if (!incrementAndCheckCount(globalCount, requestLimitGlobal)) {
                rejectRequest(rejectMeter, true, isUIQuery);
                return;
            }

            // Bind to the user
            if (!incrementAndCheckCount(count, requestLimit)) {
                // Decrement the global count that had already been incremented
                globalCount.decrementAndGet();

                rejectRequest(rejectMeter, false, isUIQuery);
                return;
            }

            // Measure the accepted request and current open connections
            requestMeter.mark();
            requestGlobalCounter.inc();

            isBound = true;
        }

        /**
         * Do the house keeping needed to reject the request.
         *
         * @param rejectMeter  Meter to count the rejection in
         * @param isRejectGlobal  Whether or not the rejection is on the global rate limit
         * @param isUIQuery  Whether or not the request is a UI Query
         */
        private void rejectRequest(Meter rejectMeter, boolean isRejectGlobal, boolean isUIQuery) {
            rejectMeter.mark();
            String limitType = isRejectGlobal ? "GLOBAL" : (isUIQuery ? "UI" : "USER");
            LOG.info("{} limit {}", limitType, userName);
            isBound = false;
        }

        @Override
        public void close() {
            if (isBound) {
                isBound = false;

                // Unbind
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
            }
        }

        @Override
        protected void finalize() throws Throwable {
            try {
                if (isBound) {
                    LOG.debug("orphaned {}", userName);
                    close();
                }
            } finally {
                super.finalize();
            }
        }

        @Override
        public boolean isBound() {
            return isBound;
        }

        @Override
        public boolean bind() {
            return isBound;
        }

        @Override
        public void unBind() {
            close();
        }
    }
}
