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
 * An implementation of Availability which merges distinct record rows from multiple availabilities, using the
 * intersection of the availability of the child availabilities, taking into consideration only children whose rows
 * are expected to participate.
 * The default assumption is that the schemas of all tables will be the same, and if they are not, outer join
 * semantics are generally expected. A partition function determines which availabilities will participate given a
 * set of constraints, typically splitting on dimension values.
 * <p>
 * Note that this is for union across availabilities with the same dimension columns who's values are non-overlapping.
 * For example, two availabilities of the following
 * <pre>
 * {@code
 * +---------------------+----------------------+
 * |      part 1         |       part 2         |
 * +-------------------------+------------------+
 * | [2017-01/2017-02]   |  [2017/2018]         |
 * +---------------------+----------------------+
 *
 * If constraints participates in all parts:
 *
 * +-------------------------+
 * |  availability(1,2)      |
 * +-------------------------+
 * | [2017-01/2017-02]       |
 * +-------------------------+

 * If a query participates only in the larger partition
 *
 * +-------------------------+
 * |  availability(2)        |
 * +-------------------------+
 * | [2017-01/2018]          |
 * +-------------------------+
 * }
 * </pre>
 */
public class PartitionAvailability extends BaseCompositeAvailability implements Availability {

    private final Map<Availability, DataSourceFilter> availabilityFilters;
    private final Set<TableName> dataSourceNames;
    /**
     * Constructor.
     *
     * @param availabilityFilters  Filters to identify which availabilities apply for a set of constraints
     * Availabilities. The function is composed of the following works functions/transformations
     * <pre>
     * {@code
     * 1. Function(DataSourceConstraint) -> Boolean
     *
     *
     * 2. Function(ApiFilter) -> sef of PartitionKeys:
     *        Dimension
     *        Map<DimensionRow, PartitionKey>
     *
     *        SingleDimensionValueMap(Dimension, Map<DimensionRow, PartitionKey>)
     *
     *        apply():
     *            dimension.getSearchProvider(ApiFilters)
     *                    .findFilteredDimensionRowsPaged(
     *                            apiFilters,
     *                            PaginationParameters.EVERYTHING_IN_ONE_PAGE
     *                    ).stream()
     *                    .map(map::get)  // This will fail in potentially bad ways if the key isn't mapped
     *                    .collect(Collectors.toSet());
     *
     * 3. Map<PartitionKey, Availability>
     * }
     * </pre>
     */
    public PartitionAvailability(
            @NotNull Map<Availability, DataSourceFilter> availabilityFilters
    ) {
        this.availabilityFilters = availabilityFilters;
        this.dataSourceNames = super.getDataSourceNames();
    }

    @Override
    protected Stream<Availability> getAllDependentAvailabilities() {
        return availabilityFilters.keySet().stream();
    }

    @Override
    public Set<TableName> getDataSourceNames() {
        return dataSourceNames;
    }

    /**
     * Retrieve all available intervals for all child availabilities.
     * <p>
     * Available intervals for the same datasource columns are unioned into a <tt>SimplifiedIntervalList</tt>
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
