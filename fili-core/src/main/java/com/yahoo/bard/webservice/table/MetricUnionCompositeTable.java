// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.availability.MetricUnionAvailability;
import com.yahoo.bard.webservice.table.resolver.GranularityComparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    private static final GranularityComparator GRANULARITY_COMPARATOR = GranularityComparator.getInstance();

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
                getCoarsestTimeGrain(physicalTables),
                columns,
                logicalToPhysicalColumnNames,
                new MetricUnionAvailability(physicalTables, columns)
        );
    }

    /**
     * Returns the coarsest <tt>ZonedTimeGrain</tt> that satisfies all tables.
     * <p>
     * If the set of <tt>PhysicalTables</tt>'s is empty or the coarsest <tt>ZonedTimeGrain</tt> is not
     * compatible with all of the <tt>PhysicalTables</tt>s, throw <tt>IllegalArgumentException</tt>.
     *
     * @param physicalTables  A set of <tt>PhysicalTable</tt>s among which the coarsest <tt>ZonedTimeGrain</tt>
     * is to be returned.
     *
     * @return the coarsest <tt>ZonedTimeGrain</tt> among a set of <tt>PhysicalTables</tt>s
     * @throws IllegalArgumentException when no PhysicalTable is provided or there is not mutually satisfying grain
     * among the table's time grain
     */
    private static ZonedTimeGrain getCoarsestTimeGrain(Set<PhysicalTable> physicalTables)
            throws IllegalArgumentException {
        // sort <tt>ZonedTimeGrain</tt>s in decreasing order
        List<ZonedTimeGrain> sortedTimeGrains = physicalTables.stream()
                .sorted(GRANULARITY_COMPARATOR)
                .map(PhysicalTable::getSchema)
                .map(PhysicalTableSchema::getTimeGrain)
                .collect(Collectors.toList());

        // check to see if all <tt>ZonedTimeGrain</tt>'s is compatible with the coarsest <tt>ZonedTimeGrain</tt>
        ZonedTimeGrain coarsestTimeGrain = sortedTimeGrains.stream()
                .findFirst()
                .orElseThrow(() -> {
                        String message = "At least 1 physical table needs to be provided";
                        LOG.error(message);
                        return new IllegalArgumentException(message);
                });

        if (sortedTimeGrains.stream().anyMatch(grain -> grain.satisfiedBy(coarsestTimeGrain))) {
            String message = String.format(
                    "There is no mutually satisfying grain among: %s",
                    sortedTimeGrains
            );
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }

        return coarsestTimeGrain;
    }
}
