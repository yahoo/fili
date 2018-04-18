// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.volatility;

import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

/**
 * A function that returns volatile intervals.
 * Volatile intervals are intervals which may still not be finalized, and thus should not be cached. The normal
 * use case for this is data which is being ingested via real time ingestion, and thus which will be replaced later by
 * new historical segments.
 */
@FunctionalInterface
public interface VolatileIntervalsFunction {

    /**
     * Retrieve a list of which intervals are volatile.
     * Volatile intervals should not be cached (in the same way that partial intervals are not cached) but are
     * returned to the user (which partial intervals are typically not). Volatile intervals will typically abut or
     * overlap incomplete data because they are on the forward edge of ingestion.
     *
     * @return A simplified interval list of volatile intervals
     */
    SimplifiedIntervalList getVolatileIntervals();
}
