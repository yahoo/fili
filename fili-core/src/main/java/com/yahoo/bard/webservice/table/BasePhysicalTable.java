// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.SegmentMetadata;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.availability.ImmutableAvailability;
import com.yahoo.bard.webservice.util.IntervalUtils;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * Base Physical Table implements common PhysicalTable capabilities.
 */
public abstract class BasePhysicalTable implements PhysicalTable {
    private static final Logger LOG = LoggerFactory.getLogger(BasePhysicalTable.class);

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
    public Availability getAvailability() {
        return availability;
    }


    @Override
    public DateTime getTableAlignment() {
        return schema.getTimeGrain().roundFloor(
                IntervalUtils.firstMoment(getAvailability().getAvailableIntervals().values()).orElse(new DateTime())
        );
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
     * Update the working intervals with values from a map.
     *
     * @param segmentMetadata  A map of names of metrics and sets of intervals over which they are valid
     * @param dimensionDictionary  The dimension dictionary from which to look up dimensions by name
     */
    public void resetColumns(SegmentMetadata segmentMetadata, DimensionDictionary dimensionDictionary) {
        setAvailability(new ImmutableAvailability(
                name,
                segmentMetadata.getDimensionIntervals(),
                segmentMetadata.getMetricIntervals(),
                dimensionDictionary
        ));
    }

    protected void setAvailability(Availability availability) {
        this.availability = availability;
    }
}
