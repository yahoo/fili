// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.availability.Availability;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An interface describing a config level physical table.
 * It may be backed by a single concrete data source or not.
 */
public interface PhysicalTable extends Table {

    @Override
    String getName();

    @Override
    PhysicalTableSchema getSchema();

    /**
     * Get the name of the current table.
     *
     * @return name of the table as TableName
     */
    TableName getTableName();

    /**
     * Get the value of the actual availability for this physical table.
     *
     * @return The current actual physical availability or a runtime exception if there isn't one yet.
     */
    Availability getAvailability();

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
     * Get the columns from the schema for this physical table.
     *
     * @return The columns of this physical table
     *
     * @deprecated In favor of getting the columns directly from the schema
     */
    @Deprecated
    Set<Column> getColumns();

    /**
     * Getter for active column intervals.
     *
     * @return tableEntries map of column to set of available intervals
     *
     */
    Map<Column, List<Interval>> getAvailableIntervals();

    /**
     * Get the time grain from granularity.
     *
     * @return The time grain of this physical table
     *
     * @deprecated use getSchema().getGranularity()
     */
    @Deprecated
    ZonedTimeGrain getTimeGrain();

    /**
     * Get the granularity for the table.
     *
     * @return The granularity of this table
     */
    Granularity getGranularity();
}
