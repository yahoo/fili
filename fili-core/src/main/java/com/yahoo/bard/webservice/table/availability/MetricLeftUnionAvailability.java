// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import com.google.common.base.MoreObjects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
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

    private static final Logger LOG = LoggerFactory.getLogger(MetricLeftUnionAvailability.class);

    private final Set<Availability> representativeAvailabilities;

    /**
     * Produce a metric left union availability.
     *
     * @param representativeAvailabilities  The availability used for aggregate availability checks. (replaces
     * intersection of all)
     * @param physicalTables  The physical tables to source metrics and dimensions from.
     * @param availabilitiesToMetricNames  The map of availabilities to the metric columns in the union schema.
     *
     * @return A metric union availability decorated with an official aggregate.
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
        this.representativeAvailabilities = Collections.unmodifiableSet(representativeAvailabilities);
        validateLeftAvailabilities();
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(PhysicalDataSourceConstraint constraint) {
        return getAvailableIntervals(getRepresentativeAvailabilitiesToMetricNames(), constraint);
    }

    /**
     * Returns a map of Availability to its set of metric names that this Availability will respond with.
     *
     * @return a mapping from Availability to its responsible set of metric names
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
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (!(o instanceof MetricLeftUnionAvailability)) { return false; }
        if (!super.equals(o)) { return false; }
        final MetricLeftUnionAvailability that = (MetricLeftUnionAvailability) o;
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
                .add("representativeAvailabilities", representativeAvailabilities)
                .toString();
    }

    /**
     * This method must be called after {@link #representativeAvailabilities} has been initialized.
     */
    protected void validateLeftAvailabilities() {
        Set<Availability> missedOutAvailabilities = getMissedOutAvailabilities();
        if (!missedOutAvailabilities.isEmpty()) {
            String message = String.format("'%s' have not been configured in table", missedOutAvailabilities);
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    private Set<Availability> getMissedOutAvailabilities() {
        return getRepresentativeAvailabilities().stream()
                .filter(availability -> !getRepresentativeAvailabilitiesToMetricNames().containsKey(availability))
                .collect(Collectors.toSet());
    }
}
