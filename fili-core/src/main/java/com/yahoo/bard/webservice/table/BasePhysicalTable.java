// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * Base implementation of physical table that are shared across various types of physical tables.
 */
public abstract class BasePhysicalTable implements PhysicalTable {
    private static final Logger LOG = LoggerFactory.getLogger(BasePhysicalTable.class);

    private final TableName name;
    private final PhysicalTableSchema schema;
    private Availability availability;

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
    public Availability getAvailability() {
        return availability;
    }

    @Override
    public PhysicalTableSchema getSchema() {
        return schema;
    }

    @Override
    public DateTime getTableAlignment() {
        return getSchema().getTimeGrain().roundFloor(
                IntervalUtils.firstMoment(getAllAvailableIntervals().values()).orElse(new DateTime())
        );
    }

    @Override
    public Map<Column, List<Interval>> getAllAvailableIntervals() {
        Map<String, List<Interval>> availableIntervals = getAvailability().getAllAvailableIntervals();

        return getSchema().getColumns().stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        column -> availableIntervals.getOrDefault(
                                getSchema().getPhysicalColumnName(column.getName()),
                                new SimplifiedIntervalList()
                        )
                ));
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        return getAvailability().getAvailableIntervals(new PhysicalDataSourceConstraint(constraint, getSchema()));
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
     * @param availability  The test availability for this table
     *
     * @deprecated  Should avoid this method and refine testing strategy to remove this method
     */
    @Deprecated
    protected void setAvailability(Availability availability) {
        this.availability = availability;
    }
}
