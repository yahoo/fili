// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An extension of {@code MetricUnionAvailability} which describes a union of source availabilities determined by its
 * subset of "representative availabilities" (see below).
 * <p>
 * {@code MetricLeftUnionAvailability} behaves the same as {@code MetricUnionAvailability} except that the coalesced
 * available intervals on this availability is determined by a subset of participating {@code Availabilities}, instead
 * of all participating {@code Availabilities} as in {@code MetricUnionAvailability}. We call this single
 * {@code Availability} as "representative" Availability.
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
 * If the representative Availabilities are Availabilities 1 {@literal &} 2, Then the available intervals for the
 * following sets of metrics required by a constraint are:
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
 * |  [metric1, metric3]  |  [2017/2018]     |
 * +-------------------+---------------------+
 * </pre>
 * Note that only the intervals from Availabilities 1 {@literal &} 2 are considered and intersected; the Availability 3
 * is not involved in the intersection operation.
 * <p>
 * This class is thread-safe.
 */
public class MetricLeftUnionAvailability extends MetricUnionAvailability {

    private static final Logger LOG = LoggerFactory.getLogger(MetricLeftUnionAvailability.class);

    private final Set<Availability> representativeAvailabilities;

    /**
     * Produce a metric left union availability.
     * <p>
     * This method calls {@link #MetricLeftUnionAvailability(Set, Set, Map)}.
     *
     * @param representativeAvailabilities  The availability used for aggregate availability checks. (replaces
     * intersection of all)
     * @param physicalTables  The physical tables to source metrics and dimensions from.
     * @param availabilitiesToMetricNames  The map of availabilities to the metric columns in the union schema.
     *
     * @return a metric union availability decorated with an official aggregate.
     */
    public static MetricLeftUnionAvailability build(
            @NotNull Set<Availability> representativeAvailabilities,
            @NotNull Collection<ConfigPhysicalTable> physicalTables,
            @NotNull Map<Availability, Set<String>> availabilitiesToMetricNames
    ) {
        return new MetricLeftUnionAvailability(
                representativeAvailabilities,
                physicalTables.stream().map(ConfigPhysicalTable::getAvailability).collect(Collectors.toSet()),
                availabilitiesToMetricNames
        );
    }

    /**
     * Constructor.
     *
     * @param representativeAvailabilities  A set of participating Availabilities that determines the coalesced
     * available intervals of this entire MetricLeftUnionAvailability
     * @param availabilities  A set of {@code Availabilities} whose Dimension schemas are (typically) the same and the
     * Metric columns are unique(i.e. no overlap) on every availability
     * @param availabilitiesToMetricNames  A map of Availability to its set of metric names that this Availability will
     * respond with
     */
    public MetricLeftUnionAvailability(
            @NotNull Set<Availability> representativeAvailabilities,
            @NotNull Set<Availability> availabilities,
            @NotNull Map<Availability, Set<String>> availabilitiesToMetricNames
    ) {
        super(availabilities, availabilitiesToMetricNames);
        validateLeftAvailabilities(representativeAvailabilities, availabilities);
        this.representativeAvailabilities = ImmutableSet.copyOf(representativeAvailabilities);
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(PhysicalDataSourceConstraint constraint) {
        return getAvailableIntervals(getRepresentativeAvailabilitiesToMetricNames(), constraint);
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals() {
        return getRepresentativeAvailabilities().stream()
                .map(Availability::getAvailableIntervals)
                .reduce(SimplifiedIntervalList::intersect)
                .orElse(new SimplifiedIntervalList());
    }

    /**
     * Returns an immutable set of representative availabilities.
     *
     * @return the immutable set of representative availabilities
     */
    public Set<Availability> getRepresentativeAvailabilities() {
        return representativeAvailabilities;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof MetricLeftUnionAvailability)) {
            return false;
        }
        if (!super.equals(other)) {
            return false;
        }
        final MetricLeftUnionAvailability that = (MetricLeftUnionAvailability) other;
        return Objects.equals(getRepresentativeAvailabilities(), that.getRepresentativeAvailabilities());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getRepresentativeAvailabilities());
    }

    /**
     * Returns a string representation of this Availability.
     *
     * The format of the string is
     * "MetricLeftUnionAvailability{representativeAvailabilities=XXX}", where "XXX" is given by
     * {@link #getRepresentativeAvailabilities()}.
     *
     * @return the string representation of this Availability
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("representativeAvailabilities", getRepresentativeAvailabilities())
                .toString();
    }

    /**
     * Validates to make sure that each representative Availability belongs to a configured table.
     *
     * @throws IllegalArgumentException if there is at least one representative Availability that does not belong to any
     * configured tables
     */
    protected static void validateLeftAvailabilities(
            Set<Availability> representativeAvailabilities,
            Set<Availability> availabilities
    ) {
        Set<Availability> missedOutAvailabilities = getMissedOutAvailabilities(
                representativeAvailabilities,
                availabilities
        );
        if (!missedOutAvailabilities.isEmpty()) {
            String message = String.format("'%s' have not been configured in table", missedOutAvailabilities);
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Returns a subset of representative Availabilities that do not belong to any configured tables.
     * <p>
     * For example, if there are 3 Availabilities each of which belongs to a configure table:
     * <ul>
     *     <li> Availability1
     *     <li> Availability2
     *     <li> Availability3
     * </ul>
     * If the representative Availabilities are "Availability1", "Availability2", the method returns an empty set. If
     * the representative Availabilities are "Availability1", "Availability2, Availability4", this method returns a set
     * of single element - "Availability4"
     *
     * @param representativeAvailabilities  The set of representative Availabilities to be checked
     * @param availabilities  A set that contains Availabilities of all configured tables
     *
     * @return the subset of representative Availabilities that do not belong to any configured tables
     */
    private static Set<Availability> getMissedOutAvailabilities(
            Set<Availability> representativeAvailabilities,
            Set<Availability> availabilities
    ) {
        return representativeAvailabilities.stream()
                .filter(availability -> !availabilities.contains(availability))
                .collect(Collectors.toSet());
    }

    /**
     * Returns a map of representative Availability to its set of metric names that this Availability will respond with.
     *
     * @return a mapping from representative Availability to its responsible set of metric names
     */
    private Map<Availability, Set<String>> getRepresentativeAvailabilitiesToMetricNames() {
        return getAvailabilitiesToMetricNames().entrySet().stream()
                .filter(entry -> getRepresentativeAvailabilities().contains(entry.getKey()))
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
                        )
                );
    }
}
