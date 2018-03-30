// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import static com.yahoo.bard.webservice.util.DateTimeUtils.EARLIEST_DATETIME;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.availability.PartitionAvailability;
import com.yahoo.bard.webservice.table.resolver.DataSourceFilter;

import org.apache.commons.collections4.map.DefaultedMap;
import org.joda.time.DateTime;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An implementation of {@link PhysicalTable} backed by {@link PartitionAvailability} availability.
 */
public class PartitionCompositeTable extends BaseCompositePhysicalTable {

    /**
     * Constructor.
     *
     * @param name  Name of the physical table as TableName, also used as fact table name
     * @param timeGrain  The time grain of the table. The time grain has to satisfy all grains of the tables
     * @param columns  The columns for this table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param availabilityFilters  A map of table to filters which apply to those tables
     * @param availabilityStartDate  A mapping from Availability to a starting instance of time after which data can
     * possibly be considered
     */
    public PartitionCompositeTable(
            @NotNull TableName name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Set<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNames,
            @NotNull Map<ConfigPhysicalTable, DataSourceFilter> availabilityFilters,
            @NotNull Map<Availability, DateTime> availabilityStartDate
    ) {
        super(
                name,
                timeGrain,
                columns,
                availabilityFilters.keySet(),
                logicalToPhysicalColumnNames,
                buildAvailability(availabilityFilters, availabilityStartDate)
        );
    }

    /**
     * Constructor.
     *
     * @param name  Name of the physical table as TableName, also used as fact table name
     * @param timeGrain  The time grain of the table. The time grain has to satisfy all grains of the tables
     * @param columns  The columns for this table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param availabilityFilters  A map of table to filters which apply to those tables
     *
     * @deprecated Use {@link #PartitionCompositeTable(TableName, ZonedTimeGrain, Set, Map, Map, Map)} instead
     */
    @Deprecated
    public PartitionCompositeTable(
            @NotNull TableName name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Set<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNames,
            @NotNull Map<ConfigPhysicalTable, DataSourceFilter> availabilityFilters
    ) {
        super(
                name,
                timeGrain,
                columns,
                availabilityFilters.keySet(),
                logicalToPhysicalColumnNames,
                buildAvailability(availabilityFilters, new DefaultedMap<>(EARLIEST_DATETIME))
        );
    }

    /**
     * Build a PartitionAvailability based on a map of table based filters.
     *
     * @param dataSourceFilterMap  A map of part tables to filters
     * @param availabilityStartDate  A mapping from Availability to a starting instance of time after which data can
     * possibly be considered
     *
     * @return  The availability describing the partition
     */
    private static Availability buildAvailability(
            Map<ConfigPhysicalTable, DataSourceFilter> dataSourceFilterMap,
            Map<Availability, DateTime> availabilityStartDate
    ) {
        Map<Availability, DataSourceFilter> availabilityFilters = dataSourceFilterMap.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().getAvailability(),
                                Map.Entry::getValue
                        )
                );
        return new PartitionAvailability(availabilityFilters, availabilityStartDate);
    }
}
