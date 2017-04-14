// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.availability.PartitionAvailability;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An implementation of <tt>BaseCompositePhysicalTable</tt> backed by partition availability.
 */
public class PartitionCompositeTable extends BaseCompositePhysicalTable {
    /**
     * Constructor.
     *
     * @param name  Name of the physical table as TableName, also used as fact table name
     * @param timeGrain  The time grain of the table. The time grain has to satisfy all grains of the tables
     * @param columns  The columns for this table
     * @param physicalTables  A set of PhysicalTables that are put together under this table. The
     * tables shall have zoned time grains that all satisfy the provided timeGrain
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param partitionFunction  A function that transform a DataSourceConstraint to a set of Availabilities
     */
    public PartitionCompositeTable(
            @NotNull TableName name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Set<Column> columns,
            @NotNull Set<PhysicalTable> physicalTables,
            @NotNull Map<String, String> logicalToPhysicalColumnNames,
            @NotNull Function<DataSourceConstraint, Set<Availability>> partitionFunction
    ) {
        super(
                name,
                timeGrain,
                columns,
                physicalTables,
                logicalToPhysicalColumnNames,
                new PartitionAvailability(
                        physicalTables.stream()
                                .map(PhysicalTable::getAvailability)
                                .collect(Collectors.toSet()),
                        columns,
                        partitionFunction
                )
        );
    }
}
