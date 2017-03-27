// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.availability.MetricUnionAvailability;
import com.yahoo.bard.webservice.table.resolver.GranularityComparator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An implementation of <tt>BasePhysicalTable</tt> backed by metric union availability.
 */
public class MetricUnionCompositeTable extends BasePhysicalTable {
    /**
     * Constructor.
     *
     * @param name  Name of the physical table as TableName, also used as fact table name
     * @param columns  The columns for this table
     * @param physicalTables  A set of <tt>PhysicalTable</tt>'s
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     */
    public MetricUnionCompositeTable(
            @NotNull TableName name,
            @NotNull Set<Column> columns,
            @NotNull Set<PhysicalTable> physicalTables,
            @NotNull Map<String, String> logicalToPhysicalColumnNames
    ) {
        super(
                name,
                getCoarsestTimeGrain(physicalTables),
                columns,
                logicalToPhysicalColumnNames,
                new MetricUnionAvailability(physicalTables, columns)
        );
    }

    /**
     * Returns the coarsest <tt>ZonedTimeGrain</tt> among a set of <tt>PhysicalTables</tt>'s.
     * <p>
     * If the set of <tt>PhysicalTables</tt>'s is empty or the coarsest <tt>ZonedTimeGrain</tt> is not
     * compatible with any of the <tt>PhysicalTables</tt>'s, throw <tt>IllegalArgumentException</tt>.
     *
     * @param physicalTables  A set of <tt>PhysicalTable</tt>'s among which the coarsest <tt>ZonedTimeGrain</tt>
     * is to be returned.
     *
     * @return the coarsest <tt>ZonedTimeGrain</tt> among a set of <tt>PhysicalTables</tt>'s
     */
    private static ZonedTimeGrain getCoarsestTimeGrain(Set<PhysicalTable> physicalTables) {
        if (physicalTables.isEmpty()) {
            throw new IllegalArgumentException("At least 1 physical table needs to be provided");
        }

        GranularityComparator granularityComparator = new GranularityComparator();

        // sort tables by <tt>ZonedTimeGrain</tt> in increasing order
        List<PhysicalTable> sortedTables = physicalTables.stream()
                .sorted((table1, table2) -> granularityComparator.compare(table1, table2))
                .collect(Collectors.toList());

        // check to see if all <tt>ZonedTimeGrain</tt>'s is compatible with the coarsest <tt>ZonedTimeGrain</tt>
        ZonedTimeGrain coarsestTimeGrain = sortedTables.get(0).getSchema().getTimeGrain();
        List<PhysicalTable> incompatibles = sortedTables.stream()
                .filter(table -> !table.getSchema().getTimeGrain().satisfiedBy(coarsestTimeGrain))
                .collect(Collectors.toList());
        if (!incompatibles.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "The following ZonedTimeGrains are not compatible with the coarsest ZoneTimeGrain({}) - {}",
                            coarsestTimeGrain,
                            incompatibles.stream()
                                    .collect(
                                            Collectors.toMap(
                                                    table -> table.getSchema().getTimeGrain(),
                                                    table -> table.getTableName().asName()
                                            )
                                    )
                            )
            );
        }

        return coarsestTimeGrain;
    }
}
