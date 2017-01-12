// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.rfc.table;

import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.Column;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.ReadablePeriod;

import java.util.Map;
import java.util.Set;

public interface PhysicalTable extends Table {
    /**
     * Get a date time that the table will align to based on grain and available intervals.
     *
     * @return The time of either the first available interval of any columns in this table or now, floored to the
     * table's time grain.
     */
    DateTime getTableAlignment();

    /**
     * Determine whether or not this PhysicalTable has a mapping for a specific logical name.
     *
     * @param logicalName  Logical name to check
     * @return True if contains a non-default mapping for the logical name, false otherwise
     */
    boolean hasLogicalMapping(String logicalName);

    @Override
    String getName();

    @Override
    PhysicalTableSchema getSchema();

    /**
     * Fetch a set of intervals given a column name.
     *
     * @param columnName  Name of the column
     *
     * @return Set of intervals associated with a column, empty if column is missing
     */
    Set<Interval> getIntervalsByColumnName(String columnName);

    /**
     * Translate a logical name into a physical column name. If no translation exists (i.e. they are the same),
     * then the logical name is returned.
     * <p>
     * NOTE: This defaulting behavior <em>WILL BE REMOVED</em> in future releases.
     * <p>
     * The defaulting behavior shouldn't be hit for Dimensions that are serialized via the default serializer and are
     * not properly configured with a logical-to-physical name mapping. Dimensions that are not "normal" dimensions,
     * such as dimensions used for DimensionSpecs in queries to do mapping from fact-level dimensions to something else,
     * should likely use their own serialization strategy so as to not hit this defaulting behavior.
     *
     * @param logicalName  Logical name to lookup in physical table
     * @return Translated logicalName if applicable
     */
    String getPhysicalColumnName(String logicalName);

    /**
     * @return The columns of this physical table
     *
     * @deprecated
     */
    @Deprecated
    Set<Column> getColumns();

    /**
     * @param columnToAdd
     *
     * @return true if the column added did not previously exist
     *
     * @deprecated Columns should be created through PhysicalTableSchema, not Physical table
     */
    @Deprecated
    Boolean addColumn(Column columnToAdd);

    /**
     * Remove a column from the table schema
     *
     * @param columnToRemove
     *
     * @return true if a column was successfully removed
     *
     * @deprecated Columns should be created through PhysicalTableSchema, not Physical table
     */
    @Deprecated
    Boolean removeColumn(Column columnToRemove);

    /**
     * Get the table bucketing as a period.
     *
     * @return The table bucketing as a period
     */
    ReadablePeriod getTablePeriod();

    /**
     * Getter for active column intervals.
     *
     * @return tableEntries map of column to set of available intervals
     *
     * @deprecated Availability contract doesn't match this signature anymore
     */
    @Deprecated
    Map<Column, Set<Interval>> getAvailableIntervals();

    /**
     * Getter for working copy of the column intervals.
     *
     * @return tableEntries map of column to set of available intervals
     *
     * @deprecated Availability contract doesn't match this signature anymore
     */
    @Deprecated
    Map<Column, Set<Interval>> getWorkingIntervals();

    /**
     * Get the time grain from granularity.
     *
     * @return The time grain of this physical table
     *
     * @deprecated use getSchema().getGranularity()
     */
    @Deprecated
    ZonedTimeGrain getTimeGrain();
}
