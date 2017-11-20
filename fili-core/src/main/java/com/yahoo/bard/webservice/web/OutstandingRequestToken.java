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
 * RequestToken for successfully bound request.
 */
public class OutstandingRequestToken extends RequestToken {
    private final String userName;
    private final AtomicInteger userCount;
    private final AtomicInteger globalCount;
    private boolean isBound;

    private static int DISABLED_RATE = -1;
    private static final Logger LOG = LoggerFactory.getLogger(OutstandingRequestToken.class);

    /**
     * Bind outstanding request to token, or fail and set to unbound.
     *
     * @param user  request user
     * @param requestLimit  request limit
     * @param requestMeter  request meter
     * @param rejectMeter  reject meter
     */
    public OutstandingRequestToken(Principal user, int requestLimit, int requestLimitGlobal, AtomicInteger userCount,
            AtomicInteger globalCount, Meter requestMeter, Meter rejectMeter, Counter requestGlobalCounter)
    {
        this.userCount = userCount;
        this.globalCount = globalCount;
        userName = String.valueOf(user == null ? null : user.getName());

        // Bind globally
        if (!incrementAndCheckCount(globalCount, requestLimitGlobal)) {
            rejectRequest(rejectMeter, RateLimitType.GLOBAL);
            return;
        }

        // Bind to the user
        if (!incrementAndCheckCount(userCount, requestLimit)) {
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