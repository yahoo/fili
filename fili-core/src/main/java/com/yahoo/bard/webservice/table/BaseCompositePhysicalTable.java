// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.availability.Availability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * An implementation of BasePhysicalTable that contains multiple tables.
 */
public abstract class BaseCompositePhysicalTable extends BasePhysicalTable {

    private static final Logger LOG = LoggerFactory.getLogger(BaseCompositePhysicalTable.class);

    /**
     * Constructor.
     *
     * @param name  Name that represents set of fact table names that are put together under this table
     * @param timeGrain  The time grain of the table. The time grain has to satisfy all grains of the tables
     * @param columns  The columns for this table
     * @param physicalTables  A set of PhysicalTables that are put together under this table. The
     * tables shall have zoned time grains that all satisfy the provided timeGrain
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param availability  The Availability of this table
     */
    public BaseCompositePhysicalTable(
            @NotNull TableName name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Set<Column> columns,
            @NotNull Set<PhysicalTable> physicalTables,
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
     * Verifies that the coarsest ZonedTimeGrain satisfies all tables.
     *
     * @param timeGrain  The coarsest ZonedTimeGrain being validated
     * @param physicalTables  A set of PhysicalTables whose ZonedTimeGrains are checked to make sure
     * they all satisfies with the given coarsest ZonedTimeGrain
     *
     * @throws IllegalArgumentException when there is no mutually satisfying grain among the table's time grains
     */
    private void verifyGrainSatisfiesAllSourceTables(
            ZonedTimeGrain timeGrain,
            Set<PhysicalTable> physicalTables
    ) throws IllegalArgumentException {
        if (!physicalTables.stream()
                .map(PhysicalTable::getSchema)
                .map(PhysicalTableSchema::getTimeGrain)
                .allMatch(timeGrain::satisfies)) {
            String message = String.format(
                    "There is no mutually satisfying grain among: %s for composite table %s",
                    physicalTables.stream()
                            .map(Table::getName),
                    getTableName()
            );
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }
}
