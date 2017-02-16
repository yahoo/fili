// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.metadata.SegmentMetadata;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.availability.ImmutableAvailability;
import com.yahoo.bard.webservice.util.IntervalUtils;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * Physical Table represents a druid table.
 * Working availabilities a transactional sensibility to changing availabilities.
 */
public abstract class BasePhysicalTable implements PhysicalTable {
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalTable.class);

    private final String name;
    private final PhysicalTableSchema schema;
    private volatile Availability availability;

    @Override
    public Availability getAvailability() {
        return availability;
    }

    /**
     * Create a physical table.
     *
     * @param name  Fili name of the physical table
     * @param timeGrain  time grain of the table
     * @param columns The columns for this physical table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param availability  The availability of columns in this table
     */
    public BasePhysicalTable(
            @NotNull String name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Iterable<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNames,
            @NotNull Availability availability
    ) {
        this.name = name;
        this.availability = availability;
        this.schema = new PhysicalTableSchema(timeGrain, columns, logicalToPhysicalColumnNames);
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
                IntervalUtils.firstMoment(getAvailability().getAvailableIntervals().values()).orElse(new DateTime())
        );
    }

    // TODO check if REALLY needed
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

    @Override
    @Deprecated
    public Set<Column> getColumns() {
        return getSchema().getColumns();
    }

    /**
     * Getter for active column intervals.
     *
     * @return tableEntries map of column to set of available intervals
     *
     */
    @Override
    public Map<Column, List<Interval>> getAvailableIntervals() {
        return getAvailability().getAvailableIntervals();
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
        return getSchema().getGranularity();
    }

    /**
     * Update the working intervals with values from a map.
     *
     * @param segmentMetadata  A map of names of metrics and sets of intervals over which they are valid
     * @param dimensionDictionary  The dimension dictionary from which to look up dimensions by name
     */
    public void resetColumns(SegmentMetadata segmentMetadata, DimensionDictionary dimensionDictionary) {
        Map<String, Set<Interval>> dimensionIntervals = segmentMetadata.getDimensionIntervals();
        Map<String, Set<Interval>> metricIntervals = segmentMetadata.getMetricIntervals();
        setAvailability(new ImmutableAvailability(
                name,
                schema,
                dimensionIntervals,
                metricIntervals,
                dimensionDictionary
        ));
    }

    public void setAvailability(Availability availability) {
        this.availability = availability;
    }
}
