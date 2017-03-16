// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.availability.PartitionAvailability;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An implementation of <tt>BasePhysicalTable</tt> backed by partition availability.
 */
public class PartitionCompositeTable extends BasePhysicalTable {
    /**
     * Constructor.
     *
     * @param name  Name of the physical table as TableName, also used as fact table name
     * @param columns  The columns for this table
     * @param physicalTables  A set of <tt>PhysicalTable</tt>s
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param partitionFunction  A function that transform a DataSourceConstraint to a set of
     * Availabilities
     */
    public PartitionCompositeTable(
            @NotNull TableName name,
            @NotNull Set<Column> columns,
            @NotNull Set<PhysicalTable> physicalTables,
            @NotNull Map<String, String> logicalToPhysicalColumnNames,
            @NotNull Function<DataSourceConstraint, Set<Availability>> partitionFunction
    ) {
        super(
                name,
                getCoarsestTimeGrain(physicalTables),
                columns,
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
