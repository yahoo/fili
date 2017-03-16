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
 * An implementation of Availability that unions same columns from Druid tables together.
 */
public class PartitionAvailability implements Availability {

    private final Set<Availability> sourceAvailabilities;
    private final Set<Column> columns;
    private final Function<DataSourceConstraint, Set<Availability>> partitionFunction;

    /**
     * Constructor.
     *
     * @param sourceAvailabilities  The set of availabilities that have the same columns
     * @param columns  The set of configured columns
     * @param partitionFunction  A function that transform a DataSourceConstraint to a set of
     * Availabilities
     */
    public PartitionAvailability(
            @NotNull Set<Availability> sourceAvailabilities,
            @NotNull Set<Column> columns,
            @NotNull Function<DataSourceConstraint, Set<Availability>> partitionFunction
    ) {
        this.sourceAvailabilities = sourceAvailabilities;
        this.columns = new HashSet<>();
        columns.forEach(column -> {
            this.columns.add(new Column(column.getName()));
        });

        this.partitionFunction = partitionFunction;
    }

    @Override
    public Set<TableName> getDataSourceNames() {
        return sourceAvailabilities.stream()
                .map(Availability::getDataSourceNames)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public Map<Column, List<Interval>> getAllAvailableIntervals() {
        return sourceAvailabilities.stream()
                .map(Availability::getAllAvailableIntervals)
                .map(Map::entrySet)
                .flatMap(Set::stream)
                .filter(entry -> columns.contains(entry.getKey()) && !entry.getValue().isEmpty())
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
