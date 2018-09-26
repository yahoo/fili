// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.PartialDataHandler;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsService;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.Comparator;

/**
 * Comparator that prefers tables that contain more data in volatile requested intervals.
 */
public class VolatileTimeComparator implements Comparator<PhysicalTable> {

    private final QueryPlanningConstraint requestConstraint;
    private final PartialDataHandler partialDataHandler;
    private final VolatileIntervalsService volatileIntervalsService;

    /**
     * Builds a table comparator that compares tables based on how much data there is in their volatile intervals.
     *
     * @param requestConstraint  Contains the request constraints extracted from DataApiRequest and TemplateDruidQuery
     * @param partialDataHandler  A service for computing partial data information
     * @param volatileIntervalsService  A service to extract the intervals in a query that are volatile with respect
     * to a given table
     */
    public VolatileTimeComparator(
            QueryPlanningConstraint requestConstraint,
            PartialDataHandler partialDataHandler,
            VolatileIntervalsService volatileIntervalsService
    ) {
        this.requestConstraint = requestConstraint;
        this.partialDataHandler = partialDataHandler;
        this.volatileIntervalsService = volatileIntervalsService;
    }

    /**
     * Performs a comparison based on how much data each table has within their volatile intervals.
     *
     * @param left  The first table
     * @param right  The second table
     *
     * @return negative if left has more data in its volatile segment than right
     */
    @Override
    public int compare(PhysicalTable left, PhysicalTable right) {
        long leftVolatileDataDuration = getAvailableVolatileDataDuration(left);
        long rightVolatileDataDuration = getAvailableVolatileDataDuration(right);

        long mostCompleteVolatile = rightVolatileDataDuration - leftVolatileDataDuration;

        // A saturated cast that allows us to turn a long into an int such that any long within the range of valid
        // integers is preserved, but anything outside the range is rounded to Integer.MIN_VALUE or Integer.MAX_VALUE.
        return (int) Math.max(Math.min(Integer.MAX_VALUE, mostCompleteVolatile), Integer.MIN_VALUE);
    }

    /**
     * Computes the duration of the available data in the request intervals that are both volatile and partial for the
     * given table.
     * <p>
     * If we computed the duration of the intervals in the table that are just volatile and present, then the
     * comparator will erroneously favor those tables that have a larger volatility range.
     * <p>
     * Suppose we have two tables t1, and t2. t1 is monthly, and t2 is daily. t1's volatile intervals are
     * [01-2015/01-2016] while t2's volatile intervals are [12-2015/01-2016]. t1 is missing the bucket
     * [12-2015/01-2016], while t2 is missing the bucket[12-31-2015/01-01-2016]. Suppose we make a request for
     * [01-2015/01-2016] at the monthly grain. Both tables have equal volatility and partiality at the request grain,
     * and t2 has more data available in the partial-but-volatile range. However, a comparator that looked just at
     * volatility would favor t1, because it has 11 months of volatile-but-present data, while t2 only has 30 days of
     * volatile-but-present data, even though t2 actually has more present data.
     *
     * @param table  The table of interest
     *
     * @return The duration of those intervals in the table that are volatile at the request granularity, missing at the
     * request granularity, and present at the table granularity
     */
    private long getAvailableVolatileDataDuration(PhysicalTable table) {
        SimplifiedIntervalList requestIntervals = new SimplifiedIntervalList(requestConstraint.getIntervals());
        Granularity apiRequestGranularity = requestConstraint.getRequestGranularity();
        // First, find the volatile intervals that are also partial at the request grain.
        SimplifiedIntervalList volatilePartialRequestIntervals = partialDataHandler.findMissingTimeGrainIntervals(
                table.getAvailableIntervals(requestConstraint),
                volatileIntervalsService.getVolatileIntervals(apiRequestGranularity, requestIntervals, table),
                apiRequestGranularity
        );
        //Next find the intervals on the physical table that are available.
        SimplifiedIntervalList tableAvailability = table.getAvailableIntervals(requestConstraint);

        //Take the duration of their intersection.
        return IntervalUtils.getTotalDuration(tableAvailability.intersect(volatilePartialRequestIntervals));
    }
}
