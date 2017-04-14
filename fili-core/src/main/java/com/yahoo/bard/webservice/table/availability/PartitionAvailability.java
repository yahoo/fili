// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.Interval;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An implementation of Availability that unions same columns from Druid tables together, in a way that the same column
 * in 2 different availabilities contains different values. Therefore if only values in one of the availabilities is
 * requested we only need to consider that table's availability.
 * <p>
 * Note that this is for union across availabilities with the same dimension columns who's values are non-overlapping.
 * For example, two availabilities of the following
 * <pre>
 * {@code
 * +-------------------------+--------------------------+
 * |         column1         |         column2          |
 * +-------------------------+--------------------------+
 * | [2017-01-01/2017-02-01] |  [2018-01-01/2018-02-01] |
 * +-------------------------+--------------------------+
 *
 * +-------------------------+
 * |         column1         |
 * +-------------------------+
 * | [2017-03-01/2017-04-01] |
 * +-------------------------+
 * }
 * </pre>
 * are joined into a partition availability below
 * <pre>
 * {@code
 * +-------------------------+--------------------------+
 * | column1(availability1)  |  column1(availability2)  |
 * +-------------------------+--------------------------+
 * | [2017-01-01/2017-02-01] |  [2017-03-01/2017-04-01] |
 * +-------------------------+--------------------------+
 * }
 * </pre>
 */
public class PartitionAvailability implements Availability {

    private final Set<Availability> sourceAvailabilities;
    private final Set<Column> columns;
    private final Function<DataSourceConstraint, Set<Availability>> partitionFunction;
    private final Set<TableName> dataSourceNames;

    /**
     * Constructor.
     *
     * @param sourceAvailabilities  The set of availabilities that have the same columns whose values are
     * non-overlapping
     * @param columns  The set of configured columns
     * @param partitionFunction  A function that transform a DataSourceConstraint to a set of
     * Availabilities. The function is composed of the following works functions/transformations
     * <pre>
     * {@code
     * 1. Function(DataSourceConstraint) -> set of ApiFilters
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
            @NotNull Set<Availability> sourceAvailabilities,
            @NotNull Set<Column> columns,
            @NotNull Function<DataSourceConstraint, Set<Availability>> partitionFunction
    ) {
        this.sourceAvailabilities = sourceAvailabilities;
        this.columns = new HashSet<>(columns);
        this.partitionFunction = partitionFunction;
        this.dataSourceNames = sourceAvailabilities.stream()
                .map(Availability::getDataSourceNames)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<TableName> getDataSourceNames() {
        return dataSourceNames;
    }

    @Override
    public Map<Column, List<Interval>> getAllAvailableIntervals() {
        return sourceAvailabilities.stream()
                .map(Availability::getAllAvailableIntervals)
                .map(Map::entrySet)
                .flatMap(Set::stream)
                .filter(entry -> columns.contains(entry.getKey()))
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (value1, value2) -> SimplifiedIntervalList.simplifyIntervals(value1, value2)
                        )
                );
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraints) {
        return new SimplifiedIntervalList(
                partitionFunction.apply(constraints).stream()
                        .map(availability -> availability.getAvailableIntervals(constraints))
                        .map(i -> (Set<Interval>) new HashSet<>(i))
                        .reduce(null, IntervalUtils::getOverlappingSubintervals)
        );
    }
}
