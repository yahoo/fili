// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;

import org.joda.time.DateTimeZone;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Holds the fields needed to define a Physical Table.
 */
public class PhysicalTableDefinition {
    final TableName name;
    final ZonedTimeGrain grain;
    final Set<? extends DimensionConfig> dimensions;
    final Map<String, String> logicalToPhysicalNames;

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
            Set<? extends DimensionConfig> dimensions
    ) {
        this.name = name;
        this.grain = grain;
        this.dimensions = Collections.unmodifiableSet(dimensions);
        this.logicalToPhysicalNames = Collections.unmodifiableMap(
                dimensions
                        .stream()
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
    public PhysicalTableDefinition(TableName name, ZonelessTimeGrain grain, Set<? extends DimensionConfig> dimensions) {
        this(name, grain.buildZonedTimeGrain(DateTimeZone.UTC), dimensions);
    }

    public TableName getName() {
        return name;
    }

    public ZonedTimeGrain getGrain() {
        return grain;
    }

    public Set<? extends DimensionConfig> getDimensions() {
        return dimensions;
    }

    public Map<String, String> getLogicalToPhysicalNames() {
        return logicalToPhysicalNames;
    }
}
