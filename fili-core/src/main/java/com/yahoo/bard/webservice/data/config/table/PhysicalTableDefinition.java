// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;

import com.google.common.collect.ImmutableSet;

import org.joda.time.DateTimeZone;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Holds the fields needed to define a Physical Table.
 */
public class PhysicalTableDefinition {
    private final TableName name;
    private final ZonedTimeGrain grain;
    private final ImmutableSet<? extends DimensionConfig> dimensions;
    private final Map<String, String> logicalToPhysicalNames;

    /**
     * Define a physical table using a zoned time grain.
     *
     * @param name  The table name
     * @param grain  The zoned time grain
     * @param dimensions  The dimension configurations
     */
    public PhysicalTableDefinition(
            TableName name,
            ZonedTimeGrain grain,
            Iterable<? extends DimensionConfig> dimensions
    ) {
        this.name = name;
        this.grain = grain;
        this.dimensions = ImmutableSet.copyOf(dimensions);
        this.logicalToPhysicalNames = Collections.unmodifiableMap(
                this.dimensions.stream()
                        .collect(
                                Collectors.toMap(
                                        DimensionConfig::getApiName,
                                        dim -> {
                                            String physicalName = dim.getPhysicalName();
                                            if (physicalName == null) {
                                                throw new RuntimeException("No physical name found for dimension: "
                                                        + dim.getApiName());
                                            }
                                            return physicalName;
                                        }
                                )
                        )
        );
    }

    /**
     * Define a physical table using a zoneless time grain, defaulting it to UTC.
     *
     * @param name  The table name
     * @param grain  The zoneless time grain
     * @param dimensions  The dimension configurations
     *
     * @deprecated The time zone of a physical table should be set explicitly rather than rely on defaulting to UTC
     */
    @Deprecated
    public PhysicalTableDefinition(
            TableName name,
            ZonelessTimeGrain grain,
            Iterable<? extends DimensionConfig> dimensions
    ) {
        this(name, grain.buildZonedTimeGrain(DateTimeZone.UTC), dimensions);
    }

    public TableName getName() {
        return name;
    }

    public ZonedTimeGrain getGrain() {
        return grain;
    }

    public ImmutableSet<? extends DimensionConfig> getDimensions() {
        return dimensions;
    }

    public Map<String, String> getLogicalToPhysicalNames() {
        return logicalToPhysicalNames;
    }
}
