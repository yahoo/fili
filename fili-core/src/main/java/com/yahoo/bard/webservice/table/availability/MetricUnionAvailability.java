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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

/**
 * An implementation of availability which puts metric columns of different availabilities together so that we can
 * query different metric columns from different availabilities at the same time.
 * <p>
 * For example, two availabilities of the following
 * <pre>
 * {@code
 * +---------------+---------------+
 * | metricColumn1 | metricColumn2 |
 * +---------------+---------------+
 * | [1/10]        | [20/30]       |
 * +---------------+---------------+

 * +---------------+---------------+---------------+
 * | metricColumn3 | metricColumn4 | metricColumn5 |
 * +---------------+---------------+---------------+
 * | [5/15]        | [25/50]       | [90/100]      |
 * +---------------+---------------+---------------+
 * }
 * </pre>
 * are joined into a metric union availability below
 * <pre>
 * {@code
 * +---------------+---------------+---------------+---------------+---------------+
 * | metricColumn1 | metricColumn2 | metricColumn3 | metricColumn4 | metricColumn5 |
 * +---------------+---------------+---------------+---------------+---------------+
 * | [1/10]        | [20/30]       | [5/15]        | [25/50]       | [90/100]      |
 * +---------------+---------------+---------------+---------------+---------------+
 * }
 * </pre>
 * Note that the joining availabilities must have completely different set of metric columns.
 */
public class MetricUnionAvailability implements Availability {
    private static final Logger LOG = LoggerFactory.getLogger(MetricUnionAvailability.class);

    private final Set<TableName> dataSourceNames;
    private final Set<MetricColumn> metricColumns;
    private final Map<Availability, Set<MetricColumn>> availabilitiesToAvailableColumns;

    /**
     * Constructor.
     *
     * @param physicalTables  A set of <tt>PhysicalTable</tt>s containing overlapping metric schema
     * @param columns  The set of all configured columns, including dimension columns, that metric union availability
     * will respond with
     */
    public MetricUnionAvailability(@NotNull Set<PhysicalTable> physicalTables, @NotNull Set<Column> columns) {
        metricColumns = Utils.getSubsetByType(columns, MetricColumn.class);

        // get a map from availability to its available metric columns intersected with configured metric columns
        // i.e. metricColumns
        availabilitiesToAvailableColumns = physicalTables.stream()
                .collect(
                        Collectors.toMap(
                                PhysicalTable::getAvailability,
                                physicalTable ->
                                        Sets.intersection(
                                                Utils.getSubsetByType(
                                                        physicalTable
                                                                .getAvailability()
                                                                .getAllAvailableIntervals()
                                                                .keySet(),
                                                        MetricColumn.class
                                                ),
                                                metricColumns
                                        )
                        )
                );

        // validate metric uniqueness such that
        // each table's underlying datasource schema don't have repeated metric column
        Map<MetricColumn, Set<Availability>> duplicates = getDuplicateValue(availabilitiesToAvailableColumns);
        if (!duplicates.isEmpty()) {
            String message = String.format(
                    "While constructing MetricUnionAvailability, Metric columns are not unique - %s",
                    duplicates
            );
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }

        dataSourceNames = availabilitiesToAvailableColumns.keySet().stream()
                .map(Availability::getDataSourceNames)
                .flatMap(Set::stream)
                .collect(
                        Collectors.collectingAndThen(
                                Collectors.toSet(),
                                Collections::unmodifiableSet
                        )
                );
    }

    @Override
    public Set<TableName> getDataSourceNames() {
        return dataSourceNames;
    }

    /**
     * Combines and returns intervals of all availabilities' metric columns.
     * <p>
     * Intervals of the same metric column are associated with the same metric column key. Overlapping intervals under
     * the same metric column key are collapsed into single interval.
     *
     * @return a map of metric column to all of its available intervals in union
     */
    @Override
    public Map<Column, List<Interval>> getAllAvailableIntervals() {
        // get all availabilities
        Set<Availability> availabilities = availabilitiesToAvailableColumns.keySet();

        // take available interval maps from all availabilities and merge the maps together
        return Stream.of(availabilities)
                .flatMap(Set::stream)
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

    @Override
    public String toString() {
        return String.format("MetricUnionAvailability with data source names: [%s] and Configured metric columns: [%s]",
                dataSourceNames.stream()
                        .map(TableName::asName)
                        .collect(Collectors.joining(", ")),
                metricColumns.stream()
                        .map(Column::getName)
                        .collect(Collectors.joining(", "))
        );
    }

    /**
     * Returns duplicate values of <tt>MetricColumn</tt>s in a map of <tt>Availability</tt>
     * to a set of <tt>MetricColumn</tt>'s contained in that <tt>Availability</tt>.
     * <p>
     * The return value is a map of duplicate <tt>MetricColumn</tt> to all <tt>Availabilities</tt>' that contains
     * this <tt>MetricColumn</tt>
     * <p>
     * For example, when a map of {availability1: [metricCol1, metricCol2], availability2: [metricCol1]} is passed to
     * this method, the method returns {metricCol1: [availability1, availability2]}
     *
     * @param availabilityToAvailableColumns  A map from <tt>Availability</tt> to set of <tt>MetricColumn</tt>
     * contained in that <tt>Availability</tt>
     *
     * @return duplicate values of <tt>MetricColumn</tt>s
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
     * Given a <tt>DataSourceConstraint</tt> - DSC1, construct a map from each availability, A, in this MetricUnion to
     * its <tt>DataSourceConstraint</tt>, DSC2.
     * <p>
     * DSC2 is constructed as the intersection of metric columns between DSC1 and
     * A's available metric columns. There are cases in which the intersection is empty; this method filters out
     * map entries that maps to <tt>DataSourceConstraint</tt> with empty set of metric names.
     *
     * @param constraint  The data constraint whose contained metric columns will be intersected with availabilities'
     * metric columns
     *
     * @return A map from <tt>Availability</tt> to <tt>DataSourceConstraint</tt> with non-empty metric names
     */
    private Map<Availability, DataSourceConstraint> constructSubConstraint(DataSourceConstraint constraint) {
        return availabilitiesToAvailableColumns.entrySet().stream()
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
