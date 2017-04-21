// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.availability.MetricUnionAvailability;

import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * An implementation of <tt>PhysicalTable</tt> backed by metric union availability.
 * <p>
 * Under this composite, unioning is only legal if all metrics are uniquely sourced within the source tables.
 * Additionally, availability is true over times where all tables which participate (via their metrics being used) have
 * legal availability.
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
 * are put together into a view such as
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
public class MetricUnionCompositeTable extends BaseCompositePhysicalTable {

    /**
     * Constructor.
     *
     * @param name  Name that represents set of fact table names joined together
     * @param timeGrain  The time grain of the table. The time grain has to satisfy all grains of the tables
     * @param columns  The columns for this table
     * @param physicalTables  A set of PhysicalTables that are put together under this table. The
     * tables shall have zoned time grains that all satisfy the provided timeGrain
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     */
    public MetricUnionCompositeTable(
            @NotNull TableName name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Set<Column> columns,
            @NotNull Set<PhysicalTable> physicalTables,
            @NotNull Map<String, String> logicalToPhysicalColumnNames
    ) {
        super(
                name,
                timeGrain,
                columns,
                physicalTables,
                logicalToPhysicalColumnNames,
                new MetricUnionAvailability(physicalTables, columns)
        );
    }
}
