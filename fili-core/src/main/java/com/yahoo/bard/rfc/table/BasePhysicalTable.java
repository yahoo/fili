// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.rfc.table;

import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.util.IntervalUtils;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.ReadablePeriod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * Physical Table represents a druid table.
 */
public abstract class BasePhysicalTable implements PhysicalTable {
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalTable.class);

    String name;
    PhysicalTableSchema schema;

    public abstract Availability getAvailability();
    public abstract Availability getWorkingAvailability();

    /**
     * Create a physical table.
     *
     * @param name  Fili name of the physical table
     * @param timeGrain  time grain of the table
     * @param columns The columns for this physical table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     */
    public BasePhysicalTable(
            @NotNull String name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Set<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNames
    ) {
        this.name = name;
        this.schema = new PhysicalTableSchema(columns, timeGrain, logicalToPhysicalColumnNames);
    }

    /**
     * Get a date time that the table will align to based on grain and available intervals.
     *
     * @return The time of either the first available interval of any columns in this table or now, floored to the
     * table's time grain.
     */
    @Override
    public DateTime getTableAlignment() {
        return schema.getGranularity().roundFloor(
                IntervalUtils.firstMoment(getAvailability().values()).orElse(new DateTime())
        );
    }

    // TODO check if needed
    /**
     * Determine whether or not this PhysicalTable has a mapping for a specific logical name.
     *
     * @param logicalName  Logical name to check
     * @return True if contains a non-default mapping for the logical name, false otherwise
     */
    @Override
    public boolean hasLogicalMapping(String logicalName) {
        return schema.containsLogicalName(logicalName);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public PhysicalTableSchema getSchema() {
        return schema;
    }


    /**
     * Fetch a set of intervals given a column name.
     *
     * @param columnName  Name of the column
     *
     * @return Set of intervals associated with a column, empty if column is missing
     */
    @Override
    public Set<Interval> getIntervalsByColumnName(String columnName) {
        return new HashSet<>(getAvailability().getIntervalsByColumnName(columnName));
    }

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
    @Override
    public String getPhysicalColumnName(String logicalName) {
        if (!schema.containsLogicalName(logicalName)) {
            LOG.warn(
                    "No mapping found for logical name '{}' to physical name on table '{}'. Will use logical name as " +
                            "physical name. This is unexpected and should not happen for properly configured " +
                            "dimensions.",
                    logicalName,
                    getName()
            );
        }
        return schema.getPhysicalColumnName(logicalName);
    }

    /**
     * Translate a physical name into a logical column name. If no translation exists (i.e. they are the same),
     * then the physical name is returned.
     *
     * @param physicalName  Physical name to lookup in physical table
     * @return Translated physicalName if applicable
     */
    private Set<String> getLogicalColumnNames(String physicalName) {
        return getSchema().getLogicalColumnNames(physicalName);
    }


    /**
     * @return The columns of this physical table
     *
     * @deprecated
     */
    @Override
    @Deprecated
    public Set<Column> getColumns() {
        return getSchema();
    }

    /**
     * @param columnToAdd
     *
     * @return true if the column added did not previously exist
     *
     * @deprecated Columns should be created through PhysicalTableSchema, not Physical table
     */
    @Override
    @Deprecated
    public Boolean addColumn(Column columnToAdd) {
        throw new UnsupportedOperationException("Deprecated with a vengence");
    }

    /**
     * Add a column to the working intervals.
     *
     * @param columnToAdd  The column instance to add
     * @param intervals  The interval set to add
     *
     * @return True if the workingIntervals had this column already
     *
     * @deprecated Columns should be created through PhysicalTableSchema, not Physical table
     */
    @Deprecated
    private Boolean addColumn(Column columnToAdd, Set<Interval> intervals) {
        throw new UnsupportedOperationException("Deprecated with a vengence");
    }

    /**
     * Remove a column from the table schema
     *
     * @param columnToRemove
     *
     * @return true if a column was successfully removed
     *
     * @deprecated Columns should be created through PhysicalTableSchema, not Physical table
     */
    @Override
    @Deprecated
    public Boolean removeColumn(Column columnToRemove) {
        throw new UnsupportedOperationException("Deprecated with a vengence");
    }

    /**
     * Get the table bucketing as a period.
     *
     * @return The table bucketing as a period
     */
    @Override
    public ReadablePeriod getTablePeriod() {
        return schema.getGranularity().getPeriod();
    }

    /**
     * Getter for active column intervals.
     *
     * @return tableEntries map of column to set of available intervals
     *
     * @deprecated Availability contract doesn't match this signature anymore
     */
    @Override
    @Deprecated
    public Map<Column, Set<Interval>> getAvailableIntervals() {
        throw new UnsupportedOperationException("Contract changed, used getAvailability()");
    }

    /**
     * Getter for working copy of the column intervals.
     *
     * @return tableEntries map of column to set of available intervals
     *
     * @deprecated Availability contract doesn't match this signature anymore
     */
    @Override
    @Deprecated
    public Map<Column, Set<Interval>> getWorkingIntervals() {
        throw new UnsupportedOperationException("Contract changed, used getWorkingAvailability()");
    }
    /**
     * Get the time grain from granularity.
     *
     * @return The time grain of this physical table
     *
     * @deprecated use getSchema().getGranularity()
     */
    @Override
    @Deprecated
    public ZonedTimeGrain getTimeGrain() {
        return schema.getGranularity();
    }

    @Override
    public Granularity getGranularity() {
        return null;
    }
}
