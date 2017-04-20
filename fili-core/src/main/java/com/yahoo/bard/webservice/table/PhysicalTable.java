// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An interface describing a fact level physical table. It may be backed by a single fact table or multiple.
 */
public interface PhysicalTable extends Table {

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
     * Getter for all the available intervals for the corresponding columns configured on the table.
     *
     * @return map of column to set of available intervals
     */
    Map<Column, List<Interval>> getAllAvailableIntervals();

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
     *
     * @return Translated logicalName if applicable
     */
    String getPhysicalColumnName(String logicalName);

    /**
     * Get available intervals satisfying the given constraints.
     *
     * @param constraint  Data constraint containing columns and api filters
     *
     * @return tableEntries a simplified interval list of available interval
     */
    SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint);

    /**
     * Get the columns from the schema for this physical table.
     *
     * @return The columns of this physical table
     *
     * @deprecated In favor of getting the columns directly from the schema
     */
    @Deprecated
    default Set<Column> getColumns() {
        return getSchema().getColumns();
    }

    /**
     * Get the time grain from granularity.
     * Physical tables must have time zone associated time grains.
     *
     * @return The time grain of this physical table
     *
     * @deprecated use getSchema().getTimeGrain()
     */
    @Deprecated
    default ZonedTimeGrain getTimeGrain() {
        return getSchema().getTimeGrain();
    }
}
