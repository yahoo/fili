// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.util.Utils;

import com.google.common.collect.Sets;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An implementation of availability which puts metric columns of different availabilities together so that we can
 * query different metric columns from different availabilities at the same time.
 * <p>
 * For example, two availabilities of the following
 * <pre>
 * {@code
 * +-------------------------+-------------------------+
 * |      metricColumn1      |      metricColumn2      |
 * +-------------------------+-------------------------+
 * | [2017-01-01/2017-02-01] | [2018-01-01/2018-02-01] |
 * +-------------------------+-------------------------+

 * +---------------------------+-------------------------+
 * |       metricColumn3       |      metricColumn4      |
 * +---------------------------+-------------------------+
 * | [[2019-01-01/2019-02-01]] | [2020-01-01/2020-02-01] |
 * +---------------------------+-------------------------+
 * }
 * </pre>
 * are joined into a metric union availability below (note that metric columns available on one availability must not
 * exist on any other availabilities.)
 * <pre>
 * {@code
 * +-------------------------+-------------------------+---------------------------+-------------------------+
 * |      metricColumn1      |      metricColumn2      |       metricColumn3       |      metricColumn4      |
 * +-------------------------+-------------------------+---------------------------+-------------------------+
 * | [2017-01-01/2017-02-01] | [2018-01-01/2018-02-01] | [[2019-01-01/2019-02-01]] | [2020-01-01/2020-02-01] |
 * +-------------------------+-------------------------+---------------------------+-------------------------+
 * }
 * </pre>
 */
public class MetricUnionAvailability implements Availability {
    private static final Logger LOG = LoggerFactory.getLogger(MetricUnionAvailability.class);

    private final Set<TableName> dataSourceNames;
    private final Set<String> metricNames;
    private final Map<Availability, Set<String>> availabilitiesToMetricNames;

    /**
     * Constructor.
     *
     * @param physicalTables  A set of <tt>PhysicalTable</tt>s whose Dimension schemas are the same and
     * the Metric columns are unique(i.e. no overlap) on every table
     * @param columns  The set of all configured columns, including dimension columns, that metric union availability
     * will respond with
     */
    public MetricUnionAvailability(@NotNull Set<PhysicalTable> physicalTables, @NotNull Set<Column> columns) {
        metricNames = Utils.getSubsetByType(columns, MetricColumn.class).stream()
                .map(MetricColumn::getName)
                .collect(Collectors.toSet());

        // Construct a map of availability to its assigned metric
        // by intersecting its underlying datasource metrics with table configured metrics
        availabilitiesToMetricNames = physicalTables.stream()
                .map(PhysicalTable::getAvailability)
                .collect(
                        Collectors.toMap(
                                Function.identity(),
                                availability ->
                                        Sets.intersection(
                                                availability.getAllAvailableIntervals().keySet(),
                                                metricNames
                                        )
                        )
                );

        dataSourceNames = availabilitiesToMetricNames.keySet().stream()
                .map(Availability::getDataSourceNames)
                .flatMap(Set::stream)
                .collect(
                        Collectors.collectingAndThen(
                                Collectors.toSet(),
                                Collections::unmodifiableSet
                        )
                );

        // validate metric uniqueness such that
        // each table's underlying datasource schema don't have repeated metric column
        if (!isMetricUnique(availabilitiesToMetricNames)) {
                String message = String.format(
                        "Metric columns must be unique across the metric union data sources, but duplicate was found " +
                                "across the following data sources: %s",
                        getDataSourceNames().stream().map(TableName::asName).collect(Collectors.joining(", "))
                );
                LOG.error(message);
                throw new RuntimeException(message);
        }
    }

    @Override
    public Set<TableName> getDataSourceNames() {
        return dataSourceNames;
    }

    /**
     * Retrieve all available intervals for all columns across all the underlying datasources.
     * <p>
     * Available intervals for the same columns are unioned into a <tt>SimplifiedIntervalList</tt>
     *
     * @return a map of column to all of its available intervals in union
     */
    @Override
    public Map<String, List<Interval>> getAllAvailableIntervals() {
        // get all availabilities take available interval maps from all availabilities and merge the maps together
        return availabilitiesToMetricNames.keySet().stream()
                .map(Availability::getAllAvailableIntervals)
                .map(Map::entrySet)
                .flatMap(Set::stream)
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (value1, value2) -> SimplifiedIntervalList.simplifyIntervals(value1, value2)
                        )
                );
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(PhysicalDataSourceConstraint constraint) {

        Set<String> dataSourceMetricNames = availabilitiesToMetricNames.values().stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        // If the table is configured with a column that is not supported by the underlying data sources
        if (!constraint.getMetricNames().stream().allMatch(dataSourceMetricNames::contains)) {
            return new SimplifiedIntervalList();
        }

        return new SimplifiedIntervalList(
                constructSubConstraint(constraint).entrySet().stream()
                        .map(entry -> entry.getKey().getAvailableIntervals(entry.getValue()))
                        .map(simplifiedIntervalList -> (Set<Interval>) new HashSet<>(simplifiedIntervalList))
                        .reduce(null, IntervalUtils::getOverlappingSubintervals)
        );
    }

    @Override
    public String toString() {
        return String.format("MetricUnionAvailability with data source names: [%s] and Configured metric columns: [%s]",
                dataSourceNames.stream()
                        .map(TableName::asName)
                        .collect(Collectors.joining(", ")),
                metricNames.stream()
                        .collect(Collectors.joining(", "))
        );
    }

    /**
     * Validates whether the metric columns are unique across each of the underlying datasource.
     *
     * @param availabilityToMetricNames  A map from <tt>Availability</tt> to set of <tt>MetricColumn</tt>
     * contained in that <tt>Availability</tt>
     *
     * @return true if metric is unique across data sources, false otherwise
     */
    private static boolean isMetricUnique(Map<Availability, Set<String>> availabilityToMetricNames) {
        Set<String> uniqueMetrics = new HashSet<>();

        return availabilityToMetricNames.values().stream()
                .flatMap(Set::stream)
                .allMatch(uniqueMetrics::add);
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
    private Map<Availability, PhysicalDataSourceConstraint> constructSubConstraint(
            PhysicalDataSourceConstraint constraint
    ) {
        return availabilitiesToMetricNames.entrySet().stream()
                .map(entry ->
                        new AbstractMap.SimpleEntry<>(
                                entry.getKey(),
                                constraint.withMetricIntersection(entry.getValue())
                        )
                )
                .filter(entry -> !entry.getValue().getMetricNames().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
