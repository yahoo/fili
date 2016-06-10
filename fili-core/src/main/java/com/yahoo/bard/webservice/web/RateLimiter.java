// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigException;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.security.Principal;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.validation.constraints.NotNull;

/**
 * Handles Rate Limiting for web service.
 */
public class RateLimiter {
    private static final Logger LOG = LoggerFactory.getLogger(RateLimiter.class);
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
    final int requestLimitGlobal;
    final int requestLimitPerUser;
    final int requestLimitUi;

    // Live count holders
    private final AtomicInteger globalCount = new AtomicInteger();
    private final ConcurrentHashMap<String, AtomicInteger> userCounts = new ConcurrentHashMap<>();

    private final Counter requestGlobalCounter;
    private final Counter usersCounter;

    private final Meter requestBypassMeter;
    private final Meter requestUiMeter;
    private final Meter requestUserMeter;
    private final Meter rejectUiMeter;
    private final Meter rejectUserMeter;

    /**
     * Loads defaults and create RateLimiter.
     *
     * @throws SystemConfigException If any parameters fail to load
     */
    public RateLimiter() throws SystemConfigException {
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
     * Type of outstanding request.
     */
    public enum RequestType {
        USER, UI, BYPASS
    }

    /**
     * Type of rate limit.
     */
    public enum RateLimitType {
        GLOBAL, USER
    }

    /**
     * Get the current count for this username.
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
     * Increment user outstanding requests count.
     *
     * @param type  request type
     * @param user  request user
     *
     * @return token holding this user's count
     */
    public RequestToken getToken(RequestType type, Principal user) {
        switch (type) {
            case UI:
                return new OutstandingRequestToken(user, requestLimitUi, requestUiMeter, rejectUiMeter);
            case USER:
                return new OutstandingRequestToken(user, requestLimitPerUser, requestUserMeter, rejectUserMeter);
            case BYPASS:
                return new BypassRequestToken();
            default:
                throw new IllegalStateException("Unknown request type " + type);
        }
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

    /**
     * Resource representing an outstanding request
     */
    public abstract class RequestToken implements Closeable {
        /**
         * Check if the token is bound
         *
         * @return true if bound or false if rejected
         */
        public abstract boolean isBound();

        /**
         * Bind the counters to the token.
         *
         * @return true if the token was able to be bound or is already bounf, or false if rejected.
         */
        public abstract boolean bind();

        /**
         * Release the token's counters.
         */
        public abstract void unBind();
    }

    /**
     * RequestToken for successfully bound request
     */
    public class OutstandingRequestToken extends RequestToken {
        final String userName;
        final AtomicInteger count;
        boolean isBound;

        /**
         * Bind outstanding request to token, or fail and set to unbound.
         *
         * @param user  request user
         * @param requestLimit  request limit
         * @param requestMeter  request meter
         * @param rejectMeter  reject meter
         */
        public OutstandingRequestToken(Principal user, int requestLimit, Meter requestMeter, Meter rejectMeter) {
            userName = String.valueOf(user == null ? null : user.getName());
            count = getCount(userName);

            // Bind globally
            if (!incrementAndCheckCount(globalCount, requestLimitGlobal)) {
                rejectRequest(rejectMeter, RateLimitType.GLOBAL);
                return;
            }

            // Bind to the user
            if (!incrementAndCheckCount(count, requestLimit)) {
                // Decrement the global count that had already been incremented
                globalCount.decrementAndGet();

                rejectRequest(rejectMeter, RateLimitType.USER);
                return;
            }

            // Measure the accepted request and current open connections
            requestMeter.mark();
            requestGlobalCounter.inc();

            isBound = true;
        }

        private void rejectRequest(Meter rejectMeter, RateLimitType limitType) {
            rejectMeter.mark();
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

    /**
     * RequestToken for rejected request
     */
    public class BypassRequestToken extends RequestToken {

        public BypassRequestToken() {
            requestBypassMeter.mark();
        }

        @Override
        public boolean isBound() {
            return true;
        }

        @Override
        public boolean bind() {
            return true;
        }

        @Override
        public void unBind() {
            // Do nothing
        }

        @Override
        public void close() {
            // Do nothing
        }
    }
}
