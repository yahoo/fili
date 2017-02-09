// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import javax.inject.Singleton;
import javax.validation.constraints.NotNull;

/**
 * Partial data handler deals with finding the missing intervals for a given request, as well as filtering out partial
 * results from the result set.
 */
@Singleton
public class PartialDataHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PartialDataHandler.class);

    /**
     * Find the holes in the passed in intervals at a given granularity.
     * <pre>
     * Interval with grain         : |--------|--------|--------|--------|--------|--------|--------|--------|
     * Dim1 intervals:               |-------------|            |--------------------------------------------|
     * Dim2 intervals:               |-------------------|  |------------------------| |---------------------|
     * Metric intervals:             |--------|                       |--------------------------------------|
     * Missing Intervals:                     |--------|--------|--------|        |--------|
     * </pre>
     * <p>
     * This method computes the subintervals of the specified intervals for which partial data is
     * present for a given combination of request metrics and dimensions (pulled from the API request and generated
     * druid query) at the specified granularity.
     *
     * @param constraints all the column names the request depends on
     * @param physicalTables  the tables whose column availabilities are checked
     * @param requestedIntervals  The intervals that may not be fully satisfied
     * @param granularity  The granularity at which to find missing intervals
     *
     * @return subintervals of the requested intervals with incomplete data
     */
    public SimplifiedIntervalList findMissingTimeGrainIntervals(
            DataSourceConstraint constraints,
            Set<PhysicalTable> physicalTables,
            @NotNull SimplifiedIntervalList requestedIntervals,
            Granularity granularity
    ) {
        SimplifiedIntervalList availableIntervals = physicalTables.stream()
                .map(table -> table.getAvailableIntervals(constraints))
                .flatMap(SimplifiedIntervalList::stream)
                .collect(SimplifiedIntervalList.getCollector());

        SimplifiedIntervalList missingIntervals = IntervalUtils.collectBucketedIntervalsNotInIntervalList(
                availableIntervals,
                requestedIntervals,
                granularity
        );

        if (granularity instanceof AllGranularity && !missingIntervals.isEmpty()) {
            missingIntervals = requestedIntervals;
        }
        LOG.debug("Missing intervals: {} for grain {}", missingIntervals, granularity);
        return missingIntervals;
    }

    /**
     * Given a table and a list of column names, get the intervals for those columns from the physical table then
     * merge into a single availability list.
     *
     * @param physicalTable  The fact source for the columns
     * @param constraints data constraint containing columns and api filters
     *
     * @return the simplified available intervals
     *
     * @deprecated use getAvailableIntervals function on physical tables instead
     */
    @Deprecated
    public SimplifiedIntervalList getAvailability(PhysicalTable physicalTable, DataSourceConstraint constraints) {
        return physicalTable.getAvailableIntervals(constraints);
    }
}
