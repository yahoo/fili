// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.availability.Availability;

import org.joda.time.DateTime;

import java.util.Set;

/**
 * An interface describing the Fili model for a fact data source (e.g. a table of dimensions and metrics).
 * It may be backed by a single concrete fact data source or by more than one with underlying joins.
 */
public interface PhysicalTable extends Table {

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
     *
     * @return True if contains a non-default mapping for the logical name, false otherwise
     *
     * @deprecated This may no longer be needed
     */
    @Deprecated
    boolean hasLogicalMapping(String logicalName);

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
     * Get the time grain from granularity.
     *
     * @return The time grain of this physical table
     *
     * @deprecated use getSchema().getGranularity()
     */
    @Deprecated
    ZonedTimeGrain getTimeGrain();

    /**
     * Get the time grain from the physical table.
     * (This is more specific than the granularity from the Table interface)
     *
     * @return A physical table schema.
     */
    PhysicalTableSchema getSchema();
}
