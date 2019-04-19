// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.physicaltables;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.availability.MetricUnionAvailability;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An implementation of {@link PhysicalTable} backed by  {@link MetricUnionAvailability}.
 * <p>
 * Under this composite, unioning is only legal if all metrics are uniquely sourced within the source tables.
 * <p>
 * For example, given two source tables with metrics such that
 * <pre>
 * {@code
 * table1:
 * +---------+---------+---------+
 * | metric1 | metric2 | metric3 |
 * +---------+---------+---------+
 *
 * table2
 * +---------+---------+---------+
 * | metric4 | metric5 | metric6 |
 * +---------+---------+---------+
 * }
 * </pre>
 * the metric schema of this composite table will look like
 * <pre>
 * {@code
 * +---------+---------+---------+---------+---------+---------+
 * | metric1 | metric2 | metric3 | metric4 | metric5 | metric6 |
 * +---------+---------+---------+---------+---------+---------+
 * }
 * </pre>
 * The available times are based on overlapping availability of participating sources provided by
 * {@link MetricUnionAvailability}.
 *
 * @see MetricUnionAvailability
 *
 * @deprecated Build BaseCompositePhysicalTable with {@link MetricUnionAvailability#build(Collection, Map)}
 */
@Deprecated
public class MetricUnionCompositeTable extends BaseCompositePhysicalTable {

    /**
     * Constructor.
     *
     * @param name  Name that represents set of fact table names joined together
     * @param timeGrain  The time grain of the table. The time grain has to satisfy all grains of the tables
     * @param columns  The columns for this table
     * @param physicalTables  A set of PhysicalTables that are put together under this table. The tables shall have
     * zoned time grains that all satisfy the provided timeGrain
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param availabilitiesToMetricNames  A map of all availabilities to set of metric names
     */
    public MetricUnionCompositeTable(
            @NotNull TableName name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Set<Column> columns,
            @NotNull Set<ConfigPhysicalTable> physicalTables,
            @NotNull Map<String, String> logicalToPhysicalColumnNames,
            @NotNull Map<Availability, Set<String>> availabilitiesToMetricNames
    ) {
        super(
                name,
                timeGrain,
                columns,
                physicalTables,
                logicalToPhysicalColumnNames,
                new MetricUnionAvailability(
                        physicalTables.stream().map(ConfigPhysicalTable::getAvailability).collect(Collectors.toSet()),
                        availabilitiesToMetricNames
                )
        );
    }
}
