// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.table.resolver.DataSourceFilter;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.Map;
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
 * The typical use is that the schemas of all sources will be the same, and if they are not, the non merging columns
 * will be expanded with null values.
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
    private final Set<TableName> dataSourceNames;

    /**
     * Constructor.
     *
     * @param availabilityFilters  A map of availabilities to filter functions that determine which requests they
     * participate in
     */
    public PartitionAvailability(
            @NotNull Map<Availability, DataSourceFilter> availabilityFilters
    ) {
        this.availabilityFilters = availabilityFilters;
        this.dataSourceNames = super.getDataSourceNames();
    }

    @Override
    protected Stream<Availability> getAllSourceAvailabilities() {
        return availabilityFilters.keySet().stream();
    }

    @Override
    public Set<TableName> getDataSourceNames() {
        return dataSourceNames;
    }

    /**
     * Retrieve the union of all available intervals for all source availabilities.
     *
     * @return a map of column to all of its available intervals in union
     */
    @Override
    public Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
        // get all availabilities take available interval maps from all availabilities and merge the maps together
        return availabilityFilters.keySet().stream()
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
    private SimplifiedIntervalList mergeAvailabilities(
            PhysicalDataSourceConstraint constraint
    ) {
        return filteredAvailabilities(constraint)
                .map(availability -> availability.getAvailableIntervals(constraint))
                .reduce(SimplifiedIntervalList::intersect).orElse(new SimplifiedIntervalList());
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(PhysicalDataSourceConstraint constraint) {
        return mergeAvailabilities(constraint);
    }
}
