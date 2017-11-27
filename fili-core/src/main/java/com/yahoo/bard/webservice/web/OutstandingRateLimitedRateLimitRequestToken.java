// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RateLimitRequestToken for successfully bound request.
 */
public class OutstandingRateLimitedRateLimitRequestToken extends RateLimitRequestToken {
    private final String userName;
    private final AtomicInteger userCount;
    private final AtomicInteger globalCount;
    private boolean isBound;

    private static int DISABLED_RATE = -1;
    private static final Logger LOG = LoggerFactory.getLogger(OutstandingRateLimitedRateLimitRequestToken.class);

    /**
     * Bind outstanding request to token, or fail and set to unbound.
     *
     * @param user  The user that issued the request.
     * @param requestLimit  The limit on the amount of requests that user can issue.
     * @param requestLimitGlobal  The limit on the amount of requests any user can issue.
     * @param userCount  The amount of in-flight requests the user hass issued.
     * @param globalCount  The total amount of in-flight requests.
     * @param requestMeter  A meter counting the total amount of accepted requests made.
     * @param rejectMeter  A meter counting the total number of rejected requests made.
     * @param requestGlobalCounter  A counter tracking the total number of accepted requests made.
     */
    public OutstandingRateLimitedRateLimitRequestToken(Principal user, int requestLimit, int requestLimitGlobal,
            AtomicInteger userCount, AtomicInteger globalCount, Meter requestMeter, Meter rejectMeter,
            Counter requestGlobalCounter
    ) {
        this.userCount = userCount;
        this.globalCount = globalCount;
        userName = String.valueOf(user == null ? null : user.getName());

        // Bind globally
        if (!incrementAndCheckCount(globalCount, requestLimitGlobal)) {
            rejectRequest(rejectMeter, DefaultRateLimitType.GLOBAL);
            return;
        }

        // Bind to the user
        if (!incrementAndCheckCount(userCount, requestLimit)) {
            // Decrement the global count that had already been incremented
            globalCount.decrementAndGet();

            rejectRequest(rejectMeter, DefaultRateLimitType.USER);
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
     * @param limitType  Type of rate limit
     */
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
            if (userCount.decrementAndGet() < 0) {
                // Reset to 0 if it falls below 0
                int old = userCount.getAndSet(0);
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

    /**
     * Sets the value of the disabled rate.
     *
     * @param disabledRate  The disabled rate to be set.
     */
    public void setDisabledRate(int disabledRate) {
        this.DISABLED_RATE = disabledRate;
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
}
