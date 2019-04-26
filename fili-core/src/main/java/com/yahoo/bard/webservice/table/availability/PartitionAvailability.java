// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.table.resolver.DataSourceFilter;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

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
 * used to determine the availability for a given {@link DataSourceConstraint}.
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

    /**
     * Constructor.
     *
     * @param availabilityFilters  A map of availabilities to filter functions that determine which requests they
     * participate in
     */
    public PartitionAvailability(@NotNull Map<Availability, DataSourceFilter> availabilityFilters) {
        super(availabilityFilters.keySet().stream());
        this.availabilityFilters = availabilityFilters;
    }

    /**
     * Return a stream of the partition parts, filtered by the associated DataSourceFilter.
     *
     * @param constraint  A constraint which filters the partitions
     *
     * @return  A stream of availabilities which participate given the constraint
     */
    private Stream<Availability> filteredAvailabilities(DataSourceConstraint constraint) {
        return availabilityFilters.entrySet().stream()
                .filter(entry -> entry.getValue().apply(constraint))
                .map(Map.Entry::getKey);
    }

    /**
     * Calculate the missing intervals based on the subpartitions availability.
     * formula is:
     * Union(subpart availabilitiy) - Union(subpart missing intervals)
     *
     * @param constraint  The filtering constraint
     *
     * @return The intervals which are available for the given constraint
     */
    private SimplifiedIntervalList mergeAvailabilities(DataSourceConstraint constraint) {
        SimplifiedIntervalList unionOfAvailableIntervals = new SimplifiedIntervalList();
        SimplifiedIntervalList unionOfMissingIntervals = new SimplifiedIntervalList();
        for (Availability availability : filteredAvailabilities(constraint).collect(Collectors.toSet())) {
            unionOfAvailableIntervals = unionOfAvailableIntervals.union(availability.getAvailableIntervals(constraint));
            unionOfMissingIntervals = unionOfMissingIntervals.union(
                    getBoundedMissingIntervalsWithConstraint(availability, constraint)
            );
        }

        return unionOfAvailableIntervals.subtract(unionOfMissingIntervals);

    }

    /**
     * Calculates the MISSING intervals for a given a availability and datasource constraint.
     *
     * @param availability  The availability to find the missing intervals for
     * @param constraint  The constraint for the query
     * @return the missing intervals
     */
    private SimplifiedIntervalList getBoundedMissingIntervalsWithConstraint(
            Availability availability,
            DataSourceConstraint constraint
    ) {
        SimplifiedIntervalList availableIntervals = availability.getAvailableIntervals(constraint);
        DateTime expectedStart = availability.getExpectedStartDate(constraint).orElse(Availability.DISTANT_PAST);
        DateTime expectedEnd = availability.getExpectedEndDate(constraint).orElse(Availability.FAR_FUTURE);
        return new SimplifiedIntervalList(Collections.singleton(new Interval(expectedStart, expectedEnd)))
                .subtract(availableIntervals);
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        return mergeAvailabilities(constraint);
    }

    @Override
    public Set<DataSourceName> getDataSourceNames(DataSourceConstraint constraint) {
        return filteredAvailabilities(constraint)
                .map(availability -> availability.getDataSourceNames(constraint))
                .flatMap(Set::stream).collect(Collectors.toSet());
    }

    @Override
    public Optional<DateTime> getExpectedStartDate(DataSourceConstraint constraint) {
        return getEarliestStart(
                constraint,
                filteredAvailabilities(constraint).collect(Collectors.toSet())
        );
    }

    @Override
    public Optional<DateTime> getExpectedEndDate(DataSourceConstraint constraint) {
        return getLatestEnd(
                constraint,
                filteredAvailabilities(constraint).collect(Collectors.toSet())
        );
    }

    /**
     * Build a PartitionAvailability based on a map of table based filters.
     *
     * @param dataSourceFilterMap  A map of part tables to filters
     *
     * @return  The availability describing the partition
     */
    public static PartitionAvailability build(Map<ConfigPhysicalTable, DataSourceFilter> dataSourceFilterMap) {
        return new PartitionAvailability(dataSourceFilterMap.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().getAvailability(),
                                Map.Entry::getValue
                        )
                )
        );
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
}
