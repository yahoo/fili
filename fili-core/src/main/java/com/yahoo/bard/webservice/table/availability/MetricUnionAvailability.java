// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import com.google.common.collect.Sets;

import org.joda.time.Interval;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An implementation of availability which which joins physical tables with the same dimension schema
 * but non-intersecting metric schemas. Metrics on the <tt>MetricUnionAvailability</tt> are sourced from
 * exactly one of the participating tables.
 */
public class MetricUnionAvailability implements Availability {

    private final Set<TableName> dataSourceNames;
    private final Set<MetricColumn> metricColumns;
    private final Map<Availability, Set<MetricColumn>> metricAvailability;

    /**
     * Constructor.
     *
     * @param physicalTables  A set of <tt>PhysicalTable</tt>'s
     * @param columns  The columns associated with all tables
     */
    public MetricUnionAvailability(@NotNull Set<PhysicalTable> physicalTables, @NotNull Set<Column> columns) {
        metricColumns =  columns.stream()
                .filter(column -> column instanceof MetricColumn)
                .map(column -> (MetricColumn) column)
                .collect(Collectors.toSet());

        metricAvailability = physicalTables.stream()
                .collect(
                        Collectors.toMap(
                                PhysicalTable::getAvailability,
                                physicalTable ->
                                        Sets.intersection(
                                                physicalTable.getSchema().getColumns(MetricColumn.class),
                                                metricColumns
                                        )
                        )
                );

        // validate metric uniqueness such that
        // each table's underlying datasource schema don't have repeated metric column
        Map<MetricColumn, Set<Availability>> duplicates = getDuplicateValue(metricAvailability);
        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Metric columns are not unique - {}",
                            duplicates
                    )
            );
        }

        dataSourceNames = physicalTables.stream()
                .map(PhysicalTable::getTableName)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<TableName> getDataSourceNames() {
        return dataSourceNames;
    }

    @Override
    public Map<Column, List<Interval>> getAllAvailableIntervals() {
        return metricAvailability.keySet().stream()
                .map(availability -> availability.getAllAvailableIntervals().entrySet())
                .flatMap(Set::stream)
                .filter(entry -> metricColumns.contains(entry.getKey()))
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey(),
                                entry -> entry.getValue(),
                                (value1, value2) -> SimplifiedIntervalList.simplifyIntervals(value1, value2)
                        )
                );
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraints) {
        return new SimplifiedIntervalList(
                constructSubConstraint(constraints).entrySet().stream()
                        .map(entry -> entry.getKey().getAvailableIntervals(entry.getValue()))
                        .map(i -> (Set<Interval>) new HashSet<>(i))
                        .reduce(null, IntervalUtils::getOverlappingSubintervals)
        );
    }

    /**
     * Returns duplicate values of <tt>MetricColumn</tt>'s in a map of <tt>Availability</tt>
     * to a set of <tt>MetricColumn</tt>'s contained in that <tt>Availability</tt>.
     * <p>
     * The return value is a map of duplicate <tt>MetricColumn</tt> to all <tt>Availability</tt>'s that contains
     * this <tt>MetricColumn</tt>
     *
     * @param availabilitySetMap  A map from <tt>Availability</tt> to set of <tt>MetricColumn</tt> contained in that
     * <tt>Availability</tt>
     *
     * @return duplicate values of <tt>MetricColumn</tt>'s
     */
    private static Map<MetricColumn, Set<Availability>> getDuplicateValue(
            Map<Availability, Set<MetricColumn>> availabilitySetMap
    ) {
        Map<MetricColumn, Set<Availability>> metricColumnSetMap = new HashMap<>();

        for (Map.Entry<Availability, Set<MetricColumn>> entry : availabilitySetMap.entrySet()) {
            Availability availability = entry.getKey();
            for (MetricColumn metricColumn : entry.getValue()) {
                metricColumnSetMap.put(
                        metricColumn,
                        Sets.union(
                                metricColumnSetMap.getOrDefault(metricColumn, new HashSet<>()),
                                Sets.newHashSet(availability)
                        )
                );
            }
        }

        return metricColumnSetMap.entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1)
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
                        )
                );
    }

    /**
     * Construct a mapping from a type of <tt>Availability</tt> to <tt>DataSourceConstraint</tt> of that availability
     * and filter out mappings that maps to <tt>DataSourceConstraint</tt> with empty set of metric names.
     *
     * @param constraint data constraint containing columns and api filters
     *
     * @return A mapping from <tt>Availability</tt> to <tt>DataSourceConstraint</tt> with non-empty metric names
     */
    private Map<Availability, DataSourceConstraint> constructSubConstraint(DataSourceConstraint constraint) {
        return getAvailabilityToConstraintMapping(constraint).entrySet().stream()
                .filter(entry -> !entry.getValue().getMetricNames().isEmpty())
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
                        )
                );
    }

    /**
     * Construct a mapping from a type of <tt>Availability</tt> to <tt>DataSourceConstraint</tt> of that availability.
     *
     * @param constraint data constraint containing columns and api filters
     *
     * @return A mapping from <tt>Availability</tt> to <tt>DataSourceConstraint</tt>
     */
    private Map<Availability, DataSourceConstraint> getAvailabilityToConstraintMapping(
            DataSourceConstraint constraint
    ) {
        return metricAvailability.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> constraint.withMetricIntersection(entry.getValue())
                        )
                );
    }
}
