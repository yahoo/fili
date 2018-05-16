// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import static com.yahoo.bard.webservice.util.DateTimeUtils.EARLIEST_DATETIME;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.table.resolver.DataSourceFilter;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.apache.commons.collections4.map.DefaultedMap;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

/**
 * An implementation of {@link Availability} which describes a union of source availabilities, filtered by request
 * parameters, such that all sources which pertain to a query are considered when calculating availability.
 * The supplied availabilityFilters also allows the query to be optimized such that only relevant availabilities are
 * used to determine the availability for a given {@link PhysicalDataSourceConstraint}.
 * <p>
 * The typical use is that the schemas of all sources will be the same, and if they are not, the non merging fields
 * will be added and filled with null values.
 * <p>
 * For example, with two source availabilities such that:
 * <pre>
 * {@code
 * +------------------+---------------+
 * |  part 1          |  part 2       |
 * +------------------+---------------+
 * |  [2017/2017-02]  |  [2017/2018]  |
 * +------------------+---------------+
 *
 * If the constraint participates in both parts:
 *
 * +---------------------+
 * |  availability(1,2)  |
 * +---------------------+
 * |  [2017/2017-02]     |
 * +---------------------+

 * If the constraint participates only in the larger partition:
 *
 * +-------------------+
 * |  availability(2)  |
 * +-------------------+
 * | [2017/2018]       |
 * +-------------------+
 * }
 * </pre>
 */
public class PartitionAvailability extends BaseCompositeAvailability implements Availability {

    private final Map<Availability, DataSourceFilter> availabilityFilters;
    private final Map<Availability, DateTime> availabilityStartDate;

    /**
     * Constructor.
     *
     * @param availabilityFilters  A map of availabilities to filter functions that determine which requests they
     * participate in
     * @param availabilityStartDate  A mapping from Availability to a starting instance of time after which data can
     * possibly be considered
     * available.
     */
    public PartitionAvailability(
            @NotNull Map<Availability, DataSourceFilter> availabilityFilters,
            Map<Availability, DateTime> availabilityStartDate
    ) {
        super(availabilityFilters.keySet().stream());
        this.availabilityFilters = availabilityFilters;
        this.availabilityStartDate = availabilityStartDate;
    }

    /**
     * Constructor.
     *
     * @param availabilityFilters  A map of availabilities to filter functions that determine which requests they
     * participate in
     *
     * @deprecated Use {@link #PartitionAvailability(Map, Map)} instead
     */
    @Deprecated
    public PartitionAvailability(@NotNull Map<Availability, DataSourceFilter> availabilityFilters) {
        super(availabilityFilters.keySet().stream());
        this.availabilityFilters = availabilityFilters;
        this.availabilityStartDate = new DefaultedMap<>(EARLIEST_DATETIME);
    }

    /**
     * Return a stream of the partition parts, filtered by the associated DataSourceFilter.
     *
     * @param constraint  A constraint which filters the partitions
     *
     * @return  A stream of availabilities which participate given the constraint
     */
    private Stream<Availability> filteredAvailabilities(PhysicalDataSourceConstraint constraint) {
        return availabilityFilters.entrySet().stream()
                .filter(entry -> entry.getValue().apply(constraint))
                .map(Map.Entry::getKey);
    }

    /**
     * Intersect the partition availabilities which participate given the constraint.
     *
     * @param constraint  The filtering constraint
     *
     * @return The intervals which are available for the given constraint
     */
    private SimplifiedIntervalList mergeAvailabilities(PhysicalDataSourceConstraint constraint) {
        return filteredAvailabilities(constraint)
                .map(availability -> getAvailableIntervals(availability, constraint))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .reduce(SimplifiedIntervalList::intersect).orElse(new SimplifiedIntervalList());
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(PhysicalDataSourceConstraint constraint) {
        return mergeAvailabilities(constraint);
    }

    @Override
    public Set<DataSourceName> getDataSourceNames(PhysicalDataSourceConstraint constraint) {
        return filteredAvailabilities(constraint)
                .map(availability -> availability.getDataSourceNames(constraint))
                .flatMap(Set::stream).collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return availabilityFilters.toString();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof PartitionAvailability) {
            PartitionAvailability that = (PartitionAvailability) obj;
            return Objects.equals(availabilityFilters, that.availabilityFilters);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(availabilityFilters);
    }

    /**
     * Returns an Optional of available intervals that should be included in the intersection operations -
     * {@link #mergeAvailabilities(PhysicalDataSourceConstraint)}
     *
     * If intervals are really missing, returns an Optional of empty simplified interval list.
     *
     * If intervals are not missing simply because they are not possibly available, returns an empty Optional.
     *
     * @param availability  An Availability object from which constrained available intervals are to be retrieved
     * @param constraint  The constraint for the available intervals
     *
     * @return an Optional of available intervals
     */
    private Optional<SimplifiedIntervalList> getAvailableIntervals(
            Availability availability,
            PhysicalDataSourceConstraint constraint
    ) {
        SimplifiedIntervalList candidateIntervals = availability.getAvailableIntervals(constraint);

        Optional<Interval> optionalEnd = availability.getAvailableIntervals(constraint).getEnd();

        // empty constraint available interval - this must be a missing interval
        if (!optionalEnd.isPresent()) {
            return Optional.of(new SimplifiedIntervalList());
        }

        // missing intervals are not possibly available - don't include in interval intersections later
        if (optionalEnd.get().isBefore(availabilityStartDate.get(availability))) {
            return Optional.empty();
        }

        SimplifiedIntervalList consideredInterval = new SimplifiedIntervalList(
                Collections.singleton(
                        new Interval(
                                availabilityStartDate.get(availability),
                                availability
                                        .getAvailableIntervals(constraint)
                                        .getEnd()
                                        .get()
                                        .getEnd()
                        )
                )
        );

        return Optional.of(candidateIntervals.intersect(consideredInterval));
    }
}
