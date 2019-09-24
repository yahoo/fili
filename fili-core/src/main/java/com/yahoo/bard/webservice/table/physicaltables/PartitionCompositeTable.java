// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.physicaltables;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.availability.PartitionAvailability;
import com.yahoo.bard.webservice.table.resolver.DataSourceFilter;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An implementation of {@link PhysicalTable} backed by {@link PartitionAvailability} availability.
 *
 * @deprecated This class is essentially a {@link BaseCompositePhysicalTable} that builds a
 * {@link PartitionAvailability}.  Instead of using this class, we should put a builder on {@link PartitionAvailability}
 * and just have the definition call the builder and create a {@link BaseCompositePhysicalTable}, to which we pass the
 * {@link PartitionAvailability}.
 */
@Deprecated
public class PartitionCompositeTable extends BaseCompositePhysicalTable {

    /**
     * Constructor.
     *
     * @param name  Name of the physical table as TableName, also used as fact table name
     * @param timeGrain  The time grain of the table. The time grain has to satisfy all grains of the tables
     * @param columns  The columns for this table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param availabilityFilters  A map of table to filters which apply to those tables
     */
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
                buildAvailability(availabilityFilters)
        );
    }

    /**
     * Build a PartitionAvailability based on a map of table based filters.
     *
     * @param dataSourceFilterMap  A map of part tables to filters
     *
     * @return  The availability describing the partition
     */
    private static Availability buildAvailability(Map<ConfigPhysicalTable, DataSourceFilter> dataSourceFilterMap) {
        return new PartitionAvailability(dataSourceFilterMap.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().getAvailability(), Map.Entry::getValue)));
    }
}
