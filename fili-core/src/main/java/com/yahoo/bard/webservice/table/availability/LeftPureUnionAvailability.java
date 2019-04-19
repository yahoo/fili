// Copyright 2019 Verizon Media Group
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.util.StreamUtils;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * A extension of {@link BaseCompositeAvailability} which describes a union of source Availabilities. The backing tables
 * <b>do not have to have the same schemas</b>; the Availabilities unions on time available for required columns. Only
 * columns from the representative table are considered. All data source names will always be reported.
 * <p>
 * The coalesced available intervals of this {@code Availability} is determined by a single participating
 * {@code Availability} which we call "<b>representative Availability</b>"
 * <p>
 * For example, with three source availabilities backing the following same metric columns:
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
 * If the representative Availability is Availability 1, then the available intervals for metric 1 required by a
 * constraint is "2017/2018". Note that only the intervals from Availability 1 counts.
 * <p>
 * This class is thread-safe.
 */
public class LeftPureUnionAvailability extends BaseCompositeAvailability {

    private final Availability representativeAvailability;

    /**
     * Constructor.
     *
     * @param representativeAvailability  A participating Availability that determines the coalesced available intervals
     * of this entire Availability
     * @param availabilities  A set of {@code Availabilities}, including the participating Availability, whose schemas
     * must be the same
     */
    public LeftPureUnionAvailability(
            @NotNull Availability representativeAvailability,
            @NotNull Set<Availability> availabilities
    ) {
        super(availabilities.stream());
        this.representativeAvailability = representativeAvailability;
    }

    // TODO - consider moving this to BaseCompositeAvailability
    @Override
    public Set<DataSourceName> getDataSourceNames(DataSourceConstraint constraint) {
        return getAllSourceAvailabilities()
                .map(availability -> availability.getDataSourceNames(constraint))
                .flatMap(Set::stream)
                .collect(
                        Collectors.collectingAndThen(
                                Collectors.toCollection(LinkedHashSet::new),
                                ImmutableSet::copyOf
                        )
                );
    }

    @Override
    public Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
        return getRepresentativeAvailability().getAllAvailableIntervals();
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals() {
        return getRepresentativeAvailability().getAvailableIntervals();
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        return getRepresentativeAvailability().getAvailableIntervals(constraint);
    }

    /**
     * Returns the representative Availability for this Availability.
     *
     * @return immutable representativeAvailability for this Availability
     */
    public Availability getRepresentativeAvailability() {
        return representativeAvailability;
    }

    /**
     * Returns a string representation of this Availability.
     * <p>
     * The format of the string is "LeftPureUnionAvailability{allAvailabilities=[XXX], dataSources=[YYY],
     * representativeAvailability=ZZZ}", where "XXX" is given by {@link #getAllSourceAvailabilities()}, and "YYY" by
     * {@link #getDataSourceNames()}, and "ZZZ" by {@link #getRepresentativeAvailability()}. Note that there is a
     * comma followed by a white space after each attribute.
     *
     * @return the string representation of this Availability
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("allAvailabilities", StreamUtils.toUnmodifiableSet(getAllSourceAvailabilities()))
                .add("dataSources", getDataSourceNames())
                .add("representativeAvailability", getRepresentativeAvailability())
                .toString();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof LeftPureUnionAvailability)) {
            return false;
        }
        final LeftPureUnionAvailability that = (LeftPureUnionAvailability) other;
        return super.equals(that) &&
                Objects.equals(getRepresentativeAvailability(), that.getRepresentativeAvailability());
    }

    @Override
    public int hashCode() {
        return Objects.hash(representativeAvailability) + super.hashCode();
    }
}
