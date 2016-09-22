// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.volatility;

import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.Interval;

import java.util.Collections;
import java.util.function.LongSupplier;

/**
 * Describe intervals as volatile from some fixed offset in the past to one in the future.
 */
public class TimeOffsetVolatileIntervalsFunction implements VolatileIntervalsFunction {

    /**
     * Closure to allow plugging time in for testing.
     */
    private static LongSupplier currentTimeMillis = System::currentTimeMillis;

    /**
     * Arbitrary future date for unbounded volatility.
     * About a century.
     */
    public static final long FAR_FUTURE = 1000L * 60 * 60 * 24 * 365 * 100;

    /**
     * The milliseconds of past time to flag as volatile.
     */
    private final long past;

    /**
     * The milliseconds of future time to flag as volatile.
     */
    private final long future;

    /**
     * Build a time offset volatile intervals function which marks all intervals from past to the value of FAR_FUTURE as
     * volatile.
     *
     * @param past  The default milliseconds before System.currentTimeMillis considered to be volatile
     */
    public TimeOffsetVolatileIntervalsFunction(long past) {
        this(past, FAR_FUTURE);
    }

    /**
     * Constructor.
     *
     * @param past  The milliseconds before System.currentTimeMillis considered to be volatile in the past
     * @param future  The milliseconds after System.currentTimeMillis considered to be volatile in the future
     */
    public TimeOffsetVolatileIntervalsFunction(long past, long future) {
        this.past = past;
        this.future = future;
    }

    @Override
    public SimplifiedIntervalList getVolatileIntervals() {
        long now = currentTimeMillis.getAsLong();
        return new SimplifiedIntervalList(Collections.singletonList(new Interval(now - past, now + future)));
    }
}
