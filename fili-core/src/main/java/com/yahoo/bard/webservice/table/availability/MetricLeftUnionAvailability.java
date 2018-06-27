// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An implementation of {@code Availability} which describes a union of source availabilities, filtered by required
 * metrics and then intersected on time available for required columns associated with a single participating
 * {@code Availability}.
 * <p>
 * {@code MetricLeftUnionAvailability} behaves the same as {@code MetricUnionAvailability} except that the coalesced
 * available intervals on this availability is determined by a single participating {@code Availability}, instead of
 * all participating {@code Availabilities} as in {@code MetricUnionAvailability}. We call this single
 * {@code Availability} as "representative" Availability.
 */
public class MetricLeftUnionAvailability extends MetricUnionAvailability {

    private final Availability representativeAvailability;

    /**
     * Constructor.
     *
     * @param representativeAvailability  A participating Availability that determines the coalesced available intervals
     * of this entire MetricLeftUnionAvailability availability
     * @param availabilities  A set of {@code Availabilities} whose Dimension schemas are (typically) the same and the
     * Metric columns are unique(i.e. no overlap) on every availability
     * @param availabilitiesToMetricNames  A map of Availability to its set of metric names that this Availability will
     * respond with
     */
    public MetricLeftUnionAvailability(
            @NotNull Availability representativeAvailability,
            @NotNull Set<Availability> availabilities,
            @NotNull Map<Availability, Set<String>> availabilitiesToMetricNames
    ) {
        super(availabilities, availabilitiesToMetricNames);
        this.representativeAvailability = representativeAvailability;
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(PhysicalDataSourceConstraint constraint) {
        Set<String> dataSourceMetricNames = getAvailabilitiesToMetricNames()
                .getOrDefault(representativeAvailability, Collections.emptySet());

        if (hasUnconfiguredMetric(constraint, dataSourceMetricNames)) {
            return new SimplifiedIntervalList();
        }

        Collection<String> representativeColumns = representativeAvailability.getAllAvailableIntervals().keySet();

        Set<String> filteredMetricNames = dataSourceMetricNames.stream()
                .filter(name -> representativeColumns.contains(name))
                .collect(Collectors.toSet());

        return representativeAvailability.getAvailableIntervals(
                constraint.withMetricIntersection(filteredMetricNames)
        );
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals() {
        return representativeAvailability.getAvailableIntervals();
    }


    /**
     * Produce a metric left union availability.
     *
     * @param representativeAvailability  The availability used for aggregate availability checks. (replaces
     * intersection of all)
     * @param physicalTables  The physical tables to source metrics and dimensions from.
     * @param availabilitiesToMetricNames  The map of availabilities to the metric columns in the union schema.
     *
     * @return A metric union availability decorated with an official aggregate.
     */

    public static MetricLeftUnionAvailability build(
            @NotNull Availability representativeAvailability,
            @NotNull Collection<ConfigPhysicalTable> physicalTables,
            @NotNull Map<Availability, Set<String>> availabilitiesToMetricNames
    ) {
        return new MetricLeftUnionAvailability(
                representativeAvailability,
                physicalTables.stream().map(ConfigPhysicalTable::getAvailability).collect(Collectors.toSet()),
                availabilitiesToMetricNames
        );
    }
}
