// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.physicaltables;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.availability.Availability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An implementation of BasePhysicalTable that contains multiple tables.
 */
public class BaseCompositePhysicalTable extends BasePhysicalTable {

    private static final Logger LOG = LoggerFactory.getLogger(BaseCompositePhysicalTable.class);

    /**
     * Constructor.
     *
     * @param name  Name that represents set of fact table names that are put together under this table
     * @param timeGrain  The time grain of the table. The time grain has to satisfy all grains of each of the tables
     * @param columns  The columns for this table
     * @param physicalTables  A set of PhysicalTables that are put together under this table. The tables shall have
     * zoned time grains that all satisfy the provided timeGrain
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param availability  The Availability of this table
     */
    public BaseCompositePhysicalTable(
            @NotNull TableName name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Set<Column> columns,
            @NotNull Set<? extends PhysicalTable> physicalTables,
            @NotNull Map<String, String> logicalToPhysicalColumnNames,
            @NotNull Availability availability
    ) {
        super(
                name,
                timeGrain,
                columns,
                logicalToPhysicalColumnNames,
                availability
        );
        verifyGrainSatisfiesAllSourceTables(getSchema().getTimeGrain(), physicalTables);
    }

    /**
     * Verifies that the ZonedTimeGrain satisfies all tables.
     *
     * @param timeGrain  The ZonedTimeGrain being validated
     * @param physicalTables  A set of PhysicalTables whose ZonedTimeGrains are checked to make sure they all satisfy
     * the given ZonedTimeGrain
     *
     * @throws IllegalArgumentException when the grain is not satisfied by the tables' time grains
     */
    private void verifyGrainSatisfiesAllSourceTables(
            ZonedTimeGrain timeGrain,
            Set<? extends PhysicalTable> physicalTables
    ) throws IllegalArgumentException {
        Predicate<PhysicalTable> tableDoesNotSatisfy = physicalTable -> !physicalTable.getSchema()
                .getTimeGrain()
                .satisfies(timeGrain);

        Set<String> unsatisfied = physicalTables.stream()
                .filter(tableDoesNotSatisfy)
                .map(PhysicalTable::getName)
                .collect(Collectors.toSet());

        if (!unsatisfied.isEmpty()) {
            String message = String.format(
                    "Time grain: '%s' cannot be satisfied by source table(s) %s",
                    timeGrain,
                    unsatisfied
            );
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }
}
