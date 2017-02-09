// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * Physical Table represents a druid table.
 * Working availabilities a transactional sensibility to changing availabilities.
 */
public abstract class BasePhysicalTable implements PhysicalTable {
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalTable.class);

    private final TableName name;
    private final PhysicalTableSchema schema;
    private volatile Availability availability;

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
            @NotNull TableName name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Iterable<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNames,
            @NotNull Availability availability
    ) {
        this.name = name;
        this.availability = availability;
        this.schema = new PhysicalTableSchema(timeGrain, columns, logicalToPhysicalColumnNames);
    }

    @Override
    public TableName getTableName() {
        return name;
    }

    @Override
    public String getName() {
        return name.asName();
    }

    @Override
    public PhysicalTableSchema getSchema() {
        return schema;
    }

    @Override
    public Granularity getGranularity() {
        return getSchema().getGranularity();
    }

    @Override
    public DateTime getTableAlignment() {
        return schema.getGranularity().roundFloor(
                IntervalUtils.firstMoment(getAvailability().getAllAvailableIntervals().values()).orElse(new DateTime())
        );
    }

    @Override
    public Availability getAvailability() {
        return availability;
    }

    @Override
    public Map<Column, Set<Interval>> getAllAvailableIntervals() {
        return getAvailability().getAllAvailableIntervals();
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraints) {
        return getAvailability().getAvailableIntervals(constraints);
    }

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
     * Used only for testing to inject test availability data into table.
     *
     * @param availability the test availability for this table
     */
    protected void setAvailability(Availability availability) {
        this.availability = availability;
    }

    @Override
    @Deprecated
    public Set<Column> getColumns() {
        return getSchema().getColumns();
    }

    @Override
    @Deprecated
    public ZonedTimeGrain getTimeGrain() {
        return schema.getGranularity();
    }
}
