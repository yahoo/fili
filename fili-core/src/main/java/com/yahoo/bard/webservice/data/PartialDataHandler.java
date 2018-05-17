// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import javax.inject.Singleton;
import javax.validation.constraints.NotNull;

/**
 * Partial data handler deals with finding the missing intervals for a given request.
 */
@Singleton
public class PartialDataHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PartialDataHandler.class);

    /**
     * Find the holes in the passed in intervals at a given granularity.
     * <pre>
     * Requested Interval with grain  |--------|--------|--------|--------|--------|--------|--------|--------|
     * Available intervals            |-------------|            |--------------------| |---------------------|
     * Missing Intervals:                      |--------|--------|--------|        |--------|
     * </pre>
     * <p>
     * Any requested subinterval bucket that is partially unavailable is collected and returned in a simplified form.
     *
     * @param availableIntervals  The intervals available in the tables
     * @param requestedIntervals  The intervals that may not be fully satisfied
     * @param granularity  The granularity at which to find missing intervals
     *
     * @return bucket-rounded subintervals of the requested intervals which have incomplete data
     */
    public SimplifiedIntervalList findMissingTimeGrainIntervals(
            @NotNull SimplifiedIntervalList availableIntervals,
            @NotNull SimplifiedIntervalList requestedIntervals,
            Granularity granularity
    ) {
        SimplifiedIntervalList missingIntervals = collectBucketedIntervalsNotInIntervalList(
                availableIntervals,
                requestedIntervals,
                granularity
        );

        // The missing intervals is the request intervals if any of the requested intervals are missing for ALL grain
        if (granularity instanceof AllGranularity && !missingIntervals.isEmpty()) {
            missingIntervals = requestedIntervals;
        }

        LOG.debug("Missing intervals: {} for grain {}", missingIntervals, granularity);
        return missingIntervals;
    }


    /**
     * Collect all subintervals from a bucketed collection that are not subintervals of a supply.
     * <p>
     * The bucketed list of intervals are split by grain before being tested as subintervals of the supply list.
     *
     * @param supplyIntervals  The intervals which bucketed intervals are being tested against
     * @param bucketedIntervals  The grain bucketed intervals to collect if not in the supply
     * @param granularity  The grain at which to split the bucketingIntervals
     *
     * @return a simplified list of intervals reflecting the intervals in the fromSet which do not appear in the
     * remove set
     */
    protected static SimplifiedIntervalList collectBucketedIntervalsNotInIntervalList(
            SimplifiedIntervalList supplyIntervals,
            SimplifiedIntervalList bucketedIntervals,
            Granularity granularity
    ) {
        // Stream the from intervals, split by grain
        Iterable<Interval> bucketIterable = granularity.intervalsIterable(bucketedIntervals);

        // Not in returns true if any part of the stream interval is not 'covered' by the remove intervals.
        Predicate<Interval> notIn = new SimplifiedIntervalList.IsSubinterval(supplyIntervals).negate();
        return StreamSupport.stream(bucketIterable.spliterator(), false)
                .filter(notIn)
                .collect(SimplifiedIntervalList.getCollector());
    }
}
