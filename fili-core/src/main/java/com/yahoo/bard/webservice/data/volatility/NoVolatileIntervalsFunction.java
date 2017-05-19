// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.volatility;

import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import javax.inject.Singleton;

/**
 * Treat no intervals as volatile.
 * This is the noop implementation of volatility. It should be access via the static instance.
 */
@Singleton
public final class NoVolatileIntervalsFunction implements VolatileIntervalsFunction {

    /**
     * Create a singleton instance of NoVolatileIntervalsFunction.
     */
    public static final NoVolatileIntervalsFunction INSTANCE = new NoVolatileIntervalsFunction();

    /**
     * Make NoVolatileIntervalsFunction a singleton by making the constructor private.
     */
    private NoVolatileIntervalsFunction() {
    }

    @Override
    public SimplifiedIntervalList getVolatileIntervals() {
        return new SimplifiedIntervalList();
    }
}
