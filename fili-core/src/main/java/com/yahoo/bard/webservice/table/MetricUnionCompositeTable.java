// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.availability.MetricUnionAvailability;
import com.yahoo.bard.webservice.util.IntervalUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * An implementation of <tt>BasePhysicalTable</tt> backed by metric union availability.
 * <p>
 * The composite table puts metric columns of different tables together so that we can
 * query different metric columns from different tables at the same time.
 * <p>
 * For example, two tables of the following
 * <pre>
 * {@code
 * table1:
 * +---------+---------+---------+
 * | metric1 | metric2 | metric3 |
 * +---------+---------+---------+
 * |         |         |         |
 * |         |         |         |
 * +---------+---------+---------+
 *
 * table2
 * +---------+---------+---------+
 * | metric4 | metric5 | metric5 |
 * +---------+---------+---------+
 * |         |         |         |
 * |         |         |         |
 * +---------+---------+---------+
 * }
 * </pre>
 * are put together into a table
 * <pre>
 * {@code
 * +---------+---------+---------+---------+---------+---------+
 * | metric1 | metric2 | metric3 | metric4 | metric5 | metric5 |
 * +---------+---------+---------+---------+---------+---------+
 * |         |         |         |         |         |         |
 * |         |         |         |         |         |         |
 * +---------+---------+---------+---------+---------+---------+
 * }
 * </pre>
 * and this joined table is backed by the <tt>MetricUnionAvailability</tt>
 *
 */
public class MetricUnionCompositeTable extends BasePhysicalTable {
    private static final Logger LOG = LoggerFactory.getLogger(MetricUnionCompositeTable.class);

    /**
     * Constructor.
     *
     * @param name  Name that represents set of fact table names joined together
     * @param columns  The columns for this table
     * @param physicalTables  A set of <tt>PhysicalTable</tt>s whose same metric schema is to be joined together. The
     * tables will be used to construct MetricUnionAvailability, as well as to compute common/coarsest time grain among
     * them. The <tt>PhysicalTable</tt>s needs to have mutually satisfying time grains in order to calculate the
     * common/coarsest time grain.
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
                IntervalUtils.getCoarsestTimeGrain(physicalTables).orElseThrow(() -> {
                    String message = String.format(
                            "At least 1 physical table needs to be provided in order to calculate " +
                                    "coarsest time grain for %s",
                            name.asName()
                    );
                    LOG.error(message);
                    return new IllegalArgumentException(message);
                }),
                columns,
                logicalToPhysicalColumnNames,
                new MetricUnionAvailability(physicalTables, columns)
        );
        verifyGrainSatisfiesAllTables(getSchema().getTimeGrain(), physicalTables, name);
    }

    /**
     * Verifies that the coarsest <tt>ZonedTimeGrain</tt> satisfies all tables.
     *
     * @param coarsestTimeGrain  The coarsest <tt>ZonedTimeGrain</tt> to be verified
     * @param physicalTables  A set of <tt>PhysicalTable</tt>s whose <tt>ZonedTimeGrain</tt>s are checked to make sure
     * they all satisfies with the given coarsest <tt>ZonedTimeGrain</tt>
     * @param name  Name of the current <tt>MetricUnionCompositeTable</tt> that represents set of fact table names
     * joined together
     *
     * @throws IllegalArgumentException when there is no mutually satisfying grain among the table's time grains
     */
    private static void verifyGrainSatisfiesAllTables(
            ZonedTimeGrain coarsestTimeGrain,
            Set<PhysicalTable> physicalTables,
            TableName name
    ) throws IllegalArgumentException {
        if (!physicalTables.stream()
                .map(PhysicalTable::getSchema)
                .map(PhysicalTableSchema::getTimeGrain)
                .allMatch(grain -> grain.satisfiedBy(coarsestTimeGrain))) {
            String message = String.format("There is no mutually satisfying grain among: %s for current table " +
                    "%s",
                    physicalTables,
                    name.asName()
            );
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }
}
