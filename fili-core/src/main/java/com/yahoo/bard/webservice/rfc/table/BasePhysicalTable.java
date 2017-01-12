// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.rfc.table;

import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.IntervalUtils;

import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * Physical Table represents a druid table.
 */
public abstract class BasePhysicalTable implements Table {
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalTable.class);

    String name;
    PhysicalTableSchema schema;

    abstract Availability getAvailability();
    abstract Availability getWorkingAvailability();

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

    public Set<Column> getColumns() {
        return getAvailability().keySet();
    }

    /**
     * Get a date time that the table will align to based on grain and available intervals.
     *
     * @return The time of either the first available interval of any columns in this table or now, floored to the
     * table's time grain.
     */
    public DateTime getTableAlignment() {
        return schema.getGranularity().roundFloor(
                IntervalUtils.firstMoment(getAvailability().values()).orElse(new DateTime())
        );
    }

    /**
     * Determine whether or not this PhysicalTable has a mapping for a specific logical name.
     *
     * @param logicalName  Logical name to check
     * @return True if contains a non-default mapping for the logical name, false otherwise
     */
    public boolean hasLogicalMapping(String logicalName) {
        return schema.containsLogicalName(logicalName);
    }

    /**
     * Get the table bucketing as a period.
     *
     * @return The table bucketing as a period
     */
    public ReadablePeriod getTablePeriod() {
        return schema.getGranularity().getPeriod();
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public PhysicalTableSchema getSchema() {
        return null;
    }
}
