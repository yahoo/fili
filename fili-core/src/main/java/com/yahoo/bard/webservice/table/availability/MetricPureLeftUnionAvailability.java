// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * A extension of {@link BaseCompositeAvailability} which describes a union of source availabilities backing the
 * <b>same datasource schema</b> and intersected on time available for required columns.
 * <p>
 * The coalesced available intervals of this {@code Availability} is determined by a subset of participating
 * {@code Availabilities} which we call "<b>representative Availabilities</b>"
 * <p>
 * For example, with three source availabilities with the following same metric(dimension columns also have to be the
 * same, but we are not showing them here):
 * <pre>
 * {@code
 * Source Availability 1:
 * +---------------+
 * |    metric1    |
 * +---------------+
 * |  [2017/2018]  |
 * +---------------+
 *
 * Source Availability 2:
 * +------------------+
 * |     metric1      |
 * +------------------+
 * |  [2016/2017-03]  |
 * +------------------+
 *
 * Source Availability 3:
 * +-----------------+
 * |     metric1     |
 * +-----------------+
 * |    2019/2020    |
 * +-----------------+
 * }
 * </pre>
 * If the representative Availabilities are Availabilities 1 {@literal &} 2, then the available intervals for metric 1
 * required by a constraint is "2017/2017-03". Note that only the intervals from Availabilities 1 {@literal &} 2 are
 * intersected to obtain the "2017/2017-03" and the Availability 3 is not involved in the intersection operation.
 * <p>
 * This class is thread-safe.
 */
public class MetricPureLeftUnionAvailability extends BaseCompositeAvailability {

    private static final Logger LOG = LoggerFactory.getLogger(MetricPureLeftUnionAvailability.class);

    private final Set<Availability> representativeAvailabilities;

    /**
     * Produce a metric left union availability.
     * <p>
     * This method calls {@link #MetricPureLeftUnionAvailability(Set, Set, Map)}.
     *
     * @param representativeAvailabilities  A set of participating Availabilities that determines the coalesced
     * available intervals of this entire MetricLeftUnionAvailability
     * @param physicalTables  The physical tables to source availabilities from
     * @param availabilitiesToColumns  The map of availabilities to the metric columns in the union schema
     *
     * @return A metric union availability decorated with an official aggregate.
     */
    public static MetricPureLeftUnionAvailability build(
            @NotNull Set<Availability> representativeAvailabilities,
            @NotNull Collection<ConfigPhysicalTable> physicalTables,
            @NotNull Map<Availability, Set<String>> availabilitiesToColumns
    ) {
        return new MetricPureLeftUnionAvailability(
                representativeAvailabilities,
                physicalTables.stream().map(ConfigPhysicalTable::getAvailability).collect(Collectors.toSet()),
                availabilitiesToColumns
        );
    }

    /**
     * Constructor.
     *
     * @param representativeAvailabilities  A set of participating Availabilities that determines the coalesced
     * available intervals of this entire MetricLeftUnionAvailability
     * @param availabilities  A set of {@code Availabilities} whose schemas must be the same
     * @param availabilitiesToColumns  The map of availabilities to the metric columns in the union schema
     */
    public MetricPureLeftUnionAvailability(
            @NotNull Set<Availability> representativeAvailabilities,
            @NotNull Set<Availability> availabilities,
            @NotNull Map<Availability, Set<String>> availabilitiesToColumns
    ) {
        super(availabilities.stream());
        validateColumns(availabilitiesToColumns);
        this.representativeAvailabilities = ImmutableSet.copyOf(representativeAvailabilities);
    }

    @Override
    public Set<DataSourceName> getDataSourceNames() {
        return getRepresentativeAvailabilities().stream()
                .map(Availability::getDataSourceNames)
                .flatMap(Set::stream)
                .collect(
                        Collectors.collectingAndThen(
                                Collectors.toCollection(LinkedHashSet::new),
                                Collections::unmodifiableSet
                        )
                );
    }

    @Override
    public Set<DataSourceName> getDataSourceNames(DataSourceConstraint constraint) {
        return getRepresentativeAvailabilities().stream()
                .map(availability -> availability.getDataSourceNames(constraint))
                .flatMap(Set::stream)
                .collect(
                        Collectors.collectingAndThen(
                                Collectors.toCollection(LinkedHashSet::new),
                                Collections::unmodifiableSet
                        )
                );
    }

    @Override
    public Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
        return getRepresentativeAvailabilities().stream()
                .map(Availability::getAllAvailableIntervals)
                .map(Map::entrySet)
                .flatMap(Set::stream)
                .collect(
                        Collectors.collectingAndThen(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue,
                                        (value1, value2) -> SimplifiedIntervalList.simplifyIntervals(value1, value2)
                                ),
                                Collections::unmodifiableMap
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

    @Override
    public SimplifiedIntervalList getAvailableIntervals(PhysicalDataSourceConstraint constraint) {
        return getRepresentativeAvailabilities().stream()
                .map(availability -> availability.getAvailableIntervals(constraint))
                .reduce(SimplifiedIntervalList::intersect)
                .orElse(new SimplifiedIntervalList());
    }

    /**
     * Returns the representative Availabilities for this Availability in an immutable set.
     *
     * @return immutable representative Availabilities set for this Availability
     */
    public Set<Availability> getRepresentativeAvailabilities() {
        return representativeAvailabilities;
    }

    /**
     * Returns a string representation of this Availability.
     *
     * The format of the string is
     * "MetricPureLeftUnionAvailability{representativeAvailabilities=XXX}", where "XXX" is given by
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

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof MetricPureLeftUnionAvailability)) {
            return false;
        }
        final MetricPureLeftUnionAvailability that = (MetricPureLeftUnionAvailability) other;
        return Objects.equals(
                getRepresentativeAvailabilities(),
                that.getRepresentativeAvailabilities()
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRepresentativeAvailabilities());
    }

    /**
     * Validates columns from multiple datasources.
     * <p>
     * The validation makes sure that
     * <ul>
     *     <li> no datasource has empty column set
     *     <li> all datasources have exactly the same column set
     * </ul>
     *
     * @param availabilitiesToColumns The map of availabilities to the metric columns in the union schema.
     *
     * @throws IllegalArgumentException if at least one datasource has empty column set or not all datasources have
     * exactly the same column set
     */
    protected static void validateColumns(Map<Availability, Set<String>> availabilitiesToColumns) {
        if (hasEmptyColSet(availabilitiesToColumns)) {
            String message = String.format("Empty column set found - '%s'", availabilitiesToColumns);
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }

        if (hasDifferentColSet(availabilitiesToColumns)) {
            String message = String.format(
                    "Columns from multiple sources do not match - '%s'",
                    availabilitiesToColumns
            );
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Returns true if at least one datasource has empty column set.
     *
     * @param availabilitiesToColumns  The map of availabilities to the metric columns in the union schema
     *
     * @return true if at least one datasource has empty column set
     */
    private static boolean hasEmptyColSet(Map<Availability, Set<String>> availabilitiesToColumns) {
        return availabilitiesToColumns.values().stream().anyMatch(Set::isEmpty);
    }

    /**
     * Returns true if not all datasources have exactly the same column set.
     *
     * @param availabilitiesToColumns  The map of availabilities to the metric columns in the union schema
     *
     * @return true if not all datasources have exactly the same column set
     */
    private static boolean hasDifferentColSet(Map<Availability, Set<String>> availabilitiesToColumns) {
        return new HashSet<>(availabilitiesToColumns.values()).size() > 1;
    }
}
