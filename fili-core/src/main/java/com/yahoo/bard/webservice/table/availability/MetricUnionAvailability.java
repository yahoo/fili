// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.google.common.collect.ImmutableSet;
import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An implementation of {@link Availability} which describes a union of source availabilities, filtered by required
 * metrics and then intersected on time available for required columns.
 * <p>
 * For example, with three source availabilities with the following metric availability:
 * <pre>
 * {@code
 * Source Availability 1:
 * +---------------+
 * |  metric1      |
 * +---------------+
 * |  [2017/2018]  |
 * +---------------+
 *
 * Source Availability 2:
 * +------------------+
 * |  metric2         |
 * +------------------+
 * |  [2016/2017-03]  |
 * +------------------+
 *
 * Source Availability 3:
 * +-----------+
 * |  metric3  |
 * +-----------+
 * |  None     |
 * +-----------+
 * }
 * </pre>
 *
 * Then the available intervals for the following sets of metrics required by a constraint are:
 * <pre>
 * +----------------------+------------------+
 * |  Requested metrics   |  Available       |
 * +----------------------+------------------+
 * |  [metric1]           |  [2017/2018]     |
 * +----------------------+------------------+
 * |  [metric2]           |  [2016/2017-03]  |
 * +----------------------+------------------+
 * |  [metric1, metric2]  | [2017/2017-03]   |
 * +----------------------+------------------+
 * |  [metric1, metric3]  |  []              |
 * +-------------------+---------------------+
 * </pre>
 */
public class MetricUnionAvailability extends BaseCompositeAvailability implements Availability {

    private static final Logger LOG = LoggerFactory.getLogger(MetricUnionAvailability.class);

    private final Set<String> metricNames;
    private final Map<Availability, Set<String>> availabilitiesToMetricNames;

    /**
     * Constructor.
     *
     * @param availabilities  A set of {@code Availabilities} whose Dimension schemas are (typically) the same and the
     * Metric columns are unique(i.e. no overlap) on every availability
     * @param availabilitiesToMetricNames  A map of Availability to its set of metric names that this Availability will
     * respond with
     */
    public MetricUnionAvailability(
            @NotNull Set<Availability> availabilities,
            @NotNull Map<Availability, Set<String>> availabilitiesToMetricNames
    ) {
        super(availabilities.stream());
        this.metricNames = availabilitiesToMetricNames.values()
                .stream()
                .flatMap(Set::stream)
                .collect(Collectors.collectingAndThen(Collectors.toSet(), ImmutableSet::copyOf));
        this.availabilitiesToMetricNames = Collections.unmodifiableMap(availabilitiesToMetricNames);

        // validate metric uniqueness such that
        // each table's underlying datasource schema don't have repeated metric column
        if (!isMetricUnique(availabilitiesToMetricNames)) {
                String message = String.format(
                        "Metric columns must be unique across the metric union data sources, but duplicate was found " +
                                "across the following data sources: %s",
                        getDataSourceNames().stream().map(DataSourceName::asName).collect(Collectors.joining(", "))
                );
                LOG.error(message);
                throw new RuntimeException(message);
        }
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(PhysicalDataSourceConstraint constraint) {

        Set<String> dataSourceMetricNames = availabilitiesToMetricNames.values().stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        if (hasUnconfiguredMetric(constraint, dataSourceMetricNames)) {
            return new SimplifiedIntervalList();
        }

        return constructSubConstraint(constraint).entrySet().stream()
                .map(entry -> entry.getKey().getAvailableIntervals(entry.getValue()))
                .reduce(SimplifiedIntervalList::intersect).orElse(new SimplifiedIntervalList());
    }

    /**
     * Returns a map of Availability to its set of metric names that this Availability will respond with.
     *
     * @return a mapping from Availability to its responsible set of metric names
     */
    public Map<Availability, Set<String>> getAvailabilitiesToMetricNames() {
        return availabilitiesToMetricNames;
    }

    /**
     * Returns true if the query constraint asks for metric column that does not exist in configured metric columns.
     *
     * @param constraint  A query constraint that contains collection of requested metric columns
     * @param configured A set of metric columns that are configured and available
     *
     * @return true if the query constraint asks for metric column that does not exist in configured metric columns
     *
     * @throws NullPointerException if either the constraint or set of configured metric names is {@code null}
     */
    protected static boolean hasUnconfiguredMetric(DataSourceConstraint constraint, Set<String> configured) {
        Objects.requireNonNull(configured);
        Objects.requireNonNull(configured);
        return !constraint.getMetricNames().stream().allMatch(configured::contains);
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
     * Returns a map from participating Availabilities to their column constrains intersected with a query constraint.
     * <p>
     * Each Availabilities new column constrains will be the intersection of their original columns and the constrained
     * columns from the query.
     *
     * @param constraint  The data constraint whose contained metric columns will be intersected with availabilities'
     * metric columns
     *
     * @return A map from Availability to DataSourceConstraint with non-empty metric names
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

    @Override
    public String toString() {
        return String.format("MetricUnionAvailability with data source names: [%s] and Configured metric columns: %s",
                getDataSourceNames().stream()
                        .map(DataSourceName::asName)
                        .collect(Collectors.joining(", ")),
                metricNames
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof MetricUnionAvailability) {
            MetricUnionAvailability that = (MetricUnionAvailability) obj;
            return Objects.equals(metricNames, that.metricNames)
                    && Objects.equals(availabilitiesToMetricNames, that.availabilitiesToMetricNames);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricNames, availabilitiesToMetricNames);
    }
}
