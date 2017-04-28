// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.Map;
import java.util.Set;

/**
 * Availability describes the intervals available by column for a table.
 */
public interface Availability {

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
    default Set<DataSourceName> getDataSourceNames(PhysicalDataSourceConstraint constraint) {
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
     * filtered by the {@link PhysicalDataSourceConstraint}.
     *
     * @param constraint  <tt>PhysicalDataSourceConstraint</tt> containing
     * {@link com.yahoo.bard.webservice.table.Schema} and {@link com.yahoo.bard.webservice.web.ApiFilter}s
     *
     * @return A <tt>SimplifiedIntervalList</tt> of intervals available
     */
    default SimplifiedIntervalList getAvailableIntervals(PhysicalDataSourceConstraint constraint) {
        return getAvailableIntervals();
    }
}
