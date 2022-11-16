// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging;

import com.yahoo.bard.webservice.web.filters.BardLoggingFilter;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Timeout helper identifies how much time is left given a timeout number.
 */
public class TimeRemainingFunction implements Function<Integer, Integer> {

    public static TimeRemainingFunction INSTANCE = new TimeRemainingFunction();

    /**
     * Calculate timeout remaining based on configured timeout and session start time.
     *
     * @param timeout  The max timeout in milliseconds.
     *
     * @return The time remaining or zero.
     */
    @Override
    public Integer apply(Integer timeout) {
        TimedPhase totalTimer =  RequestLog.fetchTiming(BardLoggingFilter.TOTAL_TIMER);
        long timeSoFarNanos = (totalTimer == null) ? 0 : totalTimer.getActiveDuration();
        return (int) Math.max(timeout - TimeUnit.NANOSECONDS.toMillis(timeSoFarNanos), 0);
    }
}
