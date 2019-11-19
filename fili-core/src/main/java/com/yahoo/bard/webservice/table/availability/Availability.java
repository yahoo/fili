// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import static org.joda.time.DateTimeZone.UTC;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.ApiFilter;

import org.joda.time.DateTime;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Availability describes the intervals available by column for a table.
 */
public interface Availability {

    // 9999-12-31 23:59
    DateTime FAR_FUTURE = new DateTime(9999, 12, 31, 23, 59, UTC);
    // 0000-01-01 00:00
    DateTime DISTANT_PAST = new DateTime(-9999, 1, 1, 0, 0, UTC);

    /**
     * The names of the data sources backing this availability.
     *
     * @return A set of names for datasources backing this availability
     */
    Set<DataSourceName> getDataSourceNames();

    /**
     * The names of the data sources backing this availability as filtered by the constraint.
     *
     * @param constraint  The constraint to filter data source names.
     *
     * @return A set of names for data sources backing this availability
     */
    default Set<DataSourceName> getDataSourceNames(DataSourceConstraint constraint) {
        return getDataSourceNames();
    }

    /**
     * The availability of all columns.
     *
     * @return The intervals, by column name, available.
     */
    Map<String, SimplifiedIntervalList> getAllAvailableIntervals();

    /**
     * Fetch a {@link SimplifiedIntervalList} representing the coalesced available intervals on this availability.
     *
     * @return A <tt>SimplifiedIntervalList</tt> of intervals available
     */
    default SimplifiedIntervalList getAvailableIntervals() {
        return getAllAvailableIntervals().values().stream()
                .reduce(SimplifiedIntervalList::union)
                .orElse(new SimplifiedIntervalList());
    }

    /**
     *
     * Fetch a {@link SimplifiedIntervalList} representing the coalesced available intervals on this availability as
     * filtered by the {@link DataSourceConstraint}.
     *
     * @param constraint  <tt>PhysicalDataSourceConstraint</tt> containing
     * {@link com.yahoo.bard.webservice.table.Schema} and {@link ApiFilter}s
     *
     * @return A <tt>SimplifiedIntervalList</tt> of intervals available
     */
    default SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        return getAvailableIntervals();
    }

    /**
     * Availability can optionally specify a date that is expected (but not enforced) to be the first date the
     * the datasource on this availability contains data. An empty optional has no defined start date.
     *
     * @param constraint the constraint to determine this availability's expected start date against
     *
     * @return A string representing the start date if present.
     *
     */
    default Optional<DateTime> getExpectedStartDate(DataSourceConstraint constraint) {
        return Optional.empty();
    }

    /**
     * Availability can optionally specify a date that is expected (but not enforced) to be the last date the
     * the datasource on this availability contains data. An empty optional has no defined end date.
     *
     * @param constraint  The constraint to determine this availability's expected end from
     *
     * @return A optional string representing the end date if present.
     *
     */
    default Optional<DateTime> getExpectedEndDate(DataSourceConstraint constraint) {
        return Optional.empty();
    }
}
