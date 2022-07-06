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
            new CallbackRateLimitRequestToken(false, () -> { }, "Exceeded user request limit.");

    protected static final RateLimitRequestToken GLOBAL_REJECT_REQUEST_TOKEN =
            new CallbackRateLimitRequestToken(false, () -> { }, "Global request limit exceeded.");

    protected static final RateLimitRequestToken BYPASS_TOKEN =
            new BypassRateLimitRequestToken();

    // Property names
    protected static final @NotNull String REQUEST_LIMIT_GLOBAL_KEY =
            SYSTEM_CONFIG.getPackageVariableName("request_limit_global");
    protected static final @NotNull String REQUEST_LIMIT_PER_USER_KEY =
            SYSTEM_CONFIG.getPackageVariableName("request_limit_per_user");
    protected static final @NotNull String EXTENDED_RATE_LIMIT_KEY =
            SYSTEM_CONFIG.getPackageVariableName("request_limit_ui");

    // Default values
    protected static final int DEFAULT_REQUEST_LIMIT_GLOBAL = 70;
    protected static final int DEFAULT_REQUEST_LIMIT_PER_USER = 2;
    protected static final int DEFAULT_REQUEST_LIMIT_EXTENDED = 52;

    protected static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();

    protected static final int DISABLED_RATE = -1;

    // Request limits
    protected final int requestLimitGlobal;
    protected final int requestLimitPerUser;
    protected final int requestLimitExtended;

    // Live count holders
    protected final AtomicInteger globalCount = new AtomicInteger();
    protected final Map<String, AtomicInteger> userCounts = new ConcurrentHashMap<>();

    protected final Counter requestGlobalCounter;
    protected final Counter usersCounter;

    protected final Meter requestBypassMeter;
    protected final Meter requestExtendedMeter;
    protected final Meter requestUserMeter;
    protected final Meter rejectExtendedMeter;
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
        requestLimitExtended = SYSTEM_CONFIG.getIntProperty(EXTENDED_RATE_LIMIT_KEY, DEFAULT_REQUEST_LIMIT_EXTENDED);

        // Register counters for currently active requests
        usersCounter = REGISTRY.counter("ratelimit.count.users");
        requestGlobalCounter = REGISTRY.counter("ratelimit.count.global");

        // Register meters for number of requests
        requestUserMeter = REGISTRY.meter("ratelimit.meter.request.user");
        requestExtendedMeter = REGISTRY.meter("ratelimit.meter.request.ui");
        requestBypassMeter = REGISTRY.meter("ratelimit.meter.request.bypass");
        rejectUserMeter = REGISTRY.meter("ratelimit.meter.reject.user");
        rejectExtendedMeter = REGISTRY.meter("ratelimit.meter.reject.ui");
    }

    /**
     * Get the current count for this username. If user does not have a counter, create one.
     *
     * @param request  The request context
     * @param isExtendedLimitQuery  Flag to check if it is a query with the extended rate limit
     * @param userName  Username to get the count for
     *
     * @return The atomic count for the user
     */
    protected AtomicInteger getCount(ContainerRequestContext request, boolean isExtendedLimitQuery, String userName) {
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
     * @param isExtendedRateLimit  Whether or not the request is a UI Query
     * @param userName  Username of the user who made the request
     */
    protected void rejectRequest(
            Meter rejectMeter,
            boolean isRejectGlobal,
            boolean isExtendedRateLimit,
            String userName
    ) {
        rejectMeter.mark();
        String limitType = isRejectGlobal ? "GLOBAL" : isExtendedRateLimit ? "UI" : "USER";
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

        // Extended rate limits support cases such as dashboard UI views
        boolean isUIQuery = useExtendedRateLimit(headers);
        Meter requestMeter;
        Meter rejectMeter;
        int requestLimit;

        if (isUIQuery) {
            requestMeter = requestExtendedMeter;
            rejectMeter = rejectExtendedMeter;
            requestLimit = requestLimitExtended;
        } else {
            requestMeter = requestUserMeter;
            rejectMeter = rejectUserMeter;
            requestLimit = requestLimitPerUser;
        }

        AtomicInteger count = getCount(request, isUIQuery, userName);
        return createNewRateLimitRequestToken(
                count,
                userName,
                isUIQuery,
                request,
                requestLimit,
                requestMeter,
                rejectMeter
        );
    }

    /**
     * Check if the extended rate limit should be used.
     *
     * @param headers Request headers to use to understand the request.
     *
     * @return true if the extended rate limit should be applied.
     */
    protected boolean useExtendedRateLimit(final MultivaluedMap<String, String> headers) {
        boolean isUIQuery = DataApiRequestTypeIdentifier.isUi(headers);
        return isUIQuery;
    }

    /**
     * Creates a new RateLimitRequestToken.
     *
     * @param count  The atomic reference that holds the amount of in-flight requests the user owns
     * @param userName  The user who launched the request
     * @param useExtendedRateLimit  Whether or not this query was generated from the UI
     * @param request  The request associated with this token
     * @param requestLimit  The limit of requests the user is allowed to launch
     * @param requestMeter  Meter tracking the amount of requests that have been launched
     * @param rejectMeter  Meter tracking the amount of requests that have been rejected
     *
     * @return a new RateLimitRequestToken, representing an in-flight (or rejected) request that is tracked by the
     * RateLimiter
     */
    protected RateLimitRequestToken createNewRateLimitRequestToken(
            AtomicInteger count,
            String userName,
            boolean useExtendedRateLimit,
            ContainerRequestContext request,
            int requestLimit,
            Meter requestMeter,
            Meter rejectMeter
    ) {
        if (!incrementAndCheckCount(globalCount, requestLimitGlobal)) {
            rejectRequest(rejectMeter, true, useExtendedRateLimit, userName);
            return getGlobalRateLimitExceededRequestToken(
                    count,
                    userName,
                    useExtendedRateLimit,
                    request,
                    requestLimit,
                    requestMeter,
                    rejectMeter
            );
        }

        // Bind to the user
        if (!incrementAndCheckCount(count, requestLimit)) {
            // Decrement the global count that had already been incremented
            globalCount.decrementAndGet();
            rejectRequest(rejectMeter, false, useExtendedRateLimit, userName);
            return getPersonalRateLimitExceededRequestToken(
                    count,
                    userName,
                    useExtendedRateLimit,
                    request,
                    requestLimit,
                    requestMeter,
                    rejectMeter
            );
        }

        // Measure the accepted request and current open connections
        requestMeter.mark();
        requestGlobalCounter.inc();

        // Return new request token
        RateLimitCleanupOnRequestComplete callback = generateCleanupClosure(count, userName, request);
        return new CallbackRateLimitRequestToken(true, callback);
    }

    /**
     * The token to reflect global rate limiting.
     * (Added to encourage extension in subclasses.)
     *
     * @param count  The atomic reference that holds the amount of in-flight requests the user owns
     * @param userName  The user who launched the request
     * @param useExtendedRateLimit  Whether or not this query was generated from the UI
     * @param request  The request associated with this token
     * @param requestLimit  The limit of requests the user is allowed to launch
     * @param requestMeter  Meter tracking the amount of requests that have been launched
     * @param rejectMeter  Meter tracking the amount of requests that have been rejected
     *
     * @return A token that indicates global request limits are full.
     */
    protected RateLimitRequestToken getPersonalRateLimitExceededRequestToken(
            AtomicInteger count,
            String userName,
            boolean useExtendedRateLimit,
            ContainerRequestContext request,
            int requestLimit,
            Meter requestMeter,
            Meter rejectMeter
    ) {
        return REJECT_REQUEST_TOKEN;
    }

    /**
     * The token to reflect global rate limiting.
     * (Added to encourage extension in subclasses.)
     *
     * @param count  The atomic reference that holds the amount of in-flight requests the user owns
     * @param userName  The user who launched the request
     * @param useExtendedRateLimit  Whether or not this query was generated from the UI
     * @param request  The request associated with this token
     * @param requestLimit  The limit of requests the user is allowed to launch
     * @param requestMeter  Meter tracking the amount of requests that have been launched
     * @param rejectMeter  Meter tracking the amount of requests that have been rejected
     *
     * @return A token that indicates global request limits are full.
     */
    protected RateLimitRequestToken getGlobalRateLimitExceededRequestToken(
            AtomicInteger count,
            String userName,
            boolean useExtendedRateLimit,
            ContainerRequestContext request,
            int requestLimit,
            Meter requestMeter,
            Meter rejectMeter
    ) {
        return GLOBAL_REJECT_REQUEST_TOKEN;
    }

    /**
     * The token to reflect global rate limiting.
     * (Added to encourage extension in subclasses.)
     *
     * @param count  The atomic reference that holds the amount of in-flight requests the user owns
     * @param userName  The user who launched the request
     * @param useExtendedRateLimit  Whether or not this query was generated from the UI
     * @param request  The request associated with this token
     * @param requestLimit  The limit of requests the user is allowed to launch
     * @param requestMeter  Meter tracking the amount of requests that have been launched
     * @param rejectMeter  Meter tracking the amount of requests that have been rejected
     *
     * @return A token that indicates global request limits are full.
     */
    protected RateLimitRequestToken getPersonalRateLimitToken(
            AtomicInteger count,
            String userName,
            boolean useExtendedRateLimit,
            ContainerRequestContext request,
            int requestLimit,
            Meter requestMeter,
            Meter rejectMeter
    ) {
        return REJECT_REQUEST_TOKEN;
    }

    /**
     * Creates a callback to be passed to a token to execute when a request has completed. The callback handles
     * decrementing the global and user counters.
     *
     * @param count  The AtomicInteger that stores the amount of in-flight requests an individual user owns
     * @param userName  The name of the user that made the request
     * @param request The request being wrapped
     *
     * @return A callback implementation to be given to a CallbackRateLimitRequestToken
     */
    protected RateLimitCleanupOnRequestComplete generateCleanupClosure(
            AtomicInteger count,
            String userName,
            ContainerRequestContext request
    ) {
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
