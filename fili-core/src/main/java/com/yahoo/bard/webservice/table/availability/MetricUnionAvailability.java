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
import com.yahoo.bard.webservice.util.Utils;

import com.google.common.collect.Sets;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An implementation of availability which joins physical tables with the same metric schema
 * but non-intersecting metric schemas. Metrics on the <tt>MetricUnionAvailability</tt> are sourced from
 * exactly one of the participating tables.
 * <p>
 * For example, two tables of the following
 *
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
 * | metric1 | metric2 | metric4 |
 * +---------+---------+---------+
 * |         |         |         |
 * |         |         |         |
 * +---------+---------+---------+
 *
 * are joined into a table
 *
 * +---------+---------+
 * | metric1 | metric2 |
 * +---------+---------+
 * |         |         |
 * |         |         |
 * +---------+---------+
 *
 * and this joined table is backed by the <tt>MetricUnionAvailability</tt>
 *
 */
public class MetricUnionAvailability implements Availability {

    private static final Logger LOG = LoggerFactory.getLogger(MetricUnionAvailability.class);

    private final Set<TableName> dataSourceNames;
    private final Set<MetricColumn> metricColumns;
    private final Map<Availability, Set<MetricColumn>> availabilityToAvailableColumns;

    /**
     * Constructor.
     *
     * @param physicalTables  A set of <tt>PhysicalTable</tt>'s whose sharing metric columns are to be joined together
     * @param columns  The set of columns supplied by this availability
     */
    public MetricUnionAvailability(@NotNull Set<PhysicalTable> physicalTables, @NotNull Set<Column> columns) {
        metricColumns =  columns.stream()
                .filter(column -> column instanceof MetricColumn)
                .map(column -> Utils.getSubsetByType(columns, MetricColumn.class))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        // get a mapping from availability to its available metric columns intersected with configured metric columns
        // i.e. metricColumns
        availabilityToAvailableColumns = physicalTables.stream()
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
        Map<MetricColumn, Set<Availability>> duplicates = getDuplicateValue(availabilityToAvailableColumns);
        if (!duplicates.isEmpty()) {
            String message = String.format(
                    "While constructing MetricUnionAvailability, Metric columns are not unique - {}",
                    duplicates
            );
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }

        dataSourceNames = availabilityToAvailableColumns.entrySet().stream()
                .map(Map.Entry::getKey)
                .map(Availability::getDataSourceNames)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<TableName> getDataSourceNames() {
        return dataSourceNames;
    }

    @Override
    public Map<Column, List<Interval>> getAllAvailableIntervals() {
        return availabilityToAvailableColumns.keySet().stream()
                .map(Availability::getAllAvailableIntervals)
                .map(Map::entrySet)
                .flatMap(Set::stream)
                .filter(entry -> metricColumns.contains(entry.getKey()))
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
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
     * <p>
     * For example, when a map of {availability1: [metricCol1, metricCol2], availability2: [metricCol1]} is passed to
     * this method, the method returns {metricCol1: [availability1, availability2]}
     *
     * @param availabilityToAvailableColumns  A map from <tt>Availability</tt> to set of <tt>MetricColumn</tt>
     * contained in that <tt>Availability</tt>
     *
     * @return duplicate values of <tt>MetricColumn</tt>'s
     */
    private static Map<MetricColumn, Set<Availability>> getDuplicateValue(
            Map<Availability, Set<MetricColumn>> availabilityToAvailableColumns
    ) {
        Map<MetricColumn, Set<Availability>> metricColumnSetMap = new HashMap<>();

        for (Map.Entry<Availability, Set<MetricColumn>> entry : availabilityToAvailableColumns.entrySet()) {
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

        // For example, when a map of {availability1: [metricCol1, metricCol2], availability2: [metricCol1]} is
        // passed to this method, at this point,
        // "metricColumnSetMap" =  {metricCol1: [availability1, availability2], {metricCol2: [availability1]}}
        // The duplicate metric columns is "metricCol1" which can be selected by knowing that the value of "metricCol1"
        // has a collection whose size is greater than 1; with that, we return
        // {metricCol1: [availability1, availability2]}
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
     * Given a <tt>DataSourceConstraint</tt> - DSC1, construct a mapping from a type of <tt>Availability</tt>, A, to its
     * <tt>DataSourceConstraint</tt>, DSC2. DSC2 is constructed as the intersection of metric columns between DSC1 and
     * A's available metric columns. There are cases in which the intersection is empty; this method filters out
     * mapping entries that maps to <tt>DataSourceConstraint</tt> with empty set of metric names.
     *
     * @param constraint data constraint whose contained metric columns will be intersected with availabilities'
     * metric columns
     *
     * @return A mapping from <tt>Availability</tt> to <tt>DataSourceConstraint</tt> with non-empty metric names
     */
    private Map<Availability, DataSourceConstraint> constructSubConstraint(DataSourceConstraint constraint) {
        return availabilityToAvailableColumns.entrySet().stream()
                .map(entry ->
                        new AbstractMap.SimpleEntry<>(
                                entry.getKey(),
                                constraint.withMetricIntersection(entry.getValue())
                        )
                )
                .filter(entry -> !entry.getValue().getMetricNames().isEmpty())
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
                        )
                );
    }
}
