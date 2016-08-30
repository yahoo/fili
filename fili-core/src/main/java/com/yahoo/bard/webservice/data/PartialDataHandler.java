// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import static com.yahoo.bard.webservice.config.BardFeatureFlag.PERMISSIVE_COLUMN_AVAILABILITY;
import static com.yahoo.bard.webservice.util.TableUtils.getColumnNames;

import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.DataApiRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
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
     * Find the request time grain intervals for which partial data is present for a given combination of request
     * metrics and dimensions (pulled from the API request and generated druid query).
     *
     * @param apiRequest  api request made by the end user
     * @param query  used to fetch data from druid
     * @param physicalTables  the tables whose column availabilities are checked
     *
     * @return list of simplified intervals with incomplete data
     *
     * @deprecated This function does the same thing as `findMissingTimeGrainIntervals`, except for an unnecessary
     * null check.
     */
    @Deprecated
    public SimplifiedIntervalList findMissingRequestTimeGrainIntervals(
            DataApiRequest apiRequest,
            DruidAggregationQuery<?> query,
            Set<PhysicalTable> physicalTables
    ) {
        // Make sure we have a list of requested intervals
        if (apiRequest.getIntervals() == null) {
            String message = "Requested interval list cannot be null";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
        return findMissingTimeGrainIntervals(
                apiRequest,
                query,
                physicalTables,
                new SimplifiedIntervalList(apiRequest.getIntervals()),
                apiRequest.getGranularity()
        );
    }

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
     * @param apiRequest  api request made by the end user
     * @param query  used to fetch data from druid
     * @param physicalTables  the tables whose column availabilities are checked
     * @param requestedIntervals  The intervals that may not be fully satisfied
     * @param granularity  The granularity at which to find missing intervals
     *
     * @return subintervals of the requested intervals with incomplete data
     */
    public SimplifiedIntervalList findMissingTimeGrainIntervals(
            DataApiRequest apiRequest,
            DruidAggregationQuery<?> query,
            Set<PhysicalTable> physicalTables,
            @NotNull SimplifiedIntervalList requestedIntervals,
            Granularity granularity
    ) {
        SimplifiedIntervalList availableIntervals = physicalTables.stream()
                .map(table -> getAvailability(table, getColumnNames(apiRequest, query, table)))
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
     * @param columnNames  The names of the columns whose availability is being checked
     *
     * @return the simplified available intervals
     */
    public SimplifiedIntervalList getAvailability(PhysicalTable physicalTable, Set<String> columnNames) {
        return PERMISSIVE_COLUMN_AVAILABILITY.isOn() ?
                getUnionSubintervalsForColumns(columnNames, physicalTable) :
                getIntersectSubintervalsForColumns(columnNames, physicalTable);
    }

    /**
     * Take a list of column names, get the intervals for those columns from the physical table then merge into a
     * single availability list by intersecting subintervals.
     *
     * @param columnNames The names of the columns in the physical table
     * @param physicalTable The physical table the columns appear in
     *
     * @return The set of all intervals fully satisfied on the request columns for the physical table
     */
    public SimplifiedIntervalList getIntersectSubintervalsForColumns(
            Collection<String> columnNames,
            PhysicalTable physicalTable
    ) {
        return columnNames.isEmpty() ?
                new SimplifiedIntervalList() :
                new SimplifiedIntervalList(columnNames.stream()
                        .map(physicalTable::getIntervalsByColumnName)
                        .reduce(null, IntervalUtils::getOverlappingSubintervals));
    }

    /**
     * Take a list of column names, get the intervals for those columns from the physical table then merge into a
     * single availability list by unioning subintervals.
     *
     * @param columnNames The names of the columns in the physical table
     * @param physicalTable The physical table the columns appear in
     *
     * @return The set of all intervals partially satisfied on the request columns for the physical table
     */
    public SimplifiedIntervalList getUnionSubintervalsForColumns(
            Collection<String> columnNames,
            PhysicalTable physicalTable
    ) {
        return columnNames.isEmpty() ?
                new SimplifiedIntervalList() :
                columnNames.stream()
                        .map(physicalTable::getIntervalsByColumnName)
                        .flatMap(Set::stream)
                        .collect(SimplifiedIntervalList.getCollector());
    }
}
