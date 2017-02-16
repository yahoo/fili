// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTableSchema;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import com.google.common.collect.ImmutableMap;

import org.joda.time.Interval;

import avro.shaded.com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An availability which guarantees immutability on its contents.
 */
//public class ImmutableAvailability extends ImmutableWrapperMap<Column, List<Interval>> implements Availability {
public class ImmutableAvailability implements Availability {

    private final TableName name;
    private final Map<Column, List<Interval>> columnIntervals;

    /**
     * Constructor.
     *
     * @param tableName The name of the data source associated with this ImmutableAvailability
     * @param map A map of columns to lists of available intervals
     */
    public ImmutableAvailability(TableName tableName, Map<Column, List<Interval>> map) {
        this.name = tableName;
        columnIntervals = ImmutableMap.copyOf(map);
    }

    /**
     * Constructor.
     *
     * @param tableName The name of the data source associated with this ImmutableAvailability
     * @param map A map of columns to lists of available intervals
     */
    public ImmutableAvailability(String tableName, Map<Column, List<Interval>> map) {
        this(TableName.of(tableName), map);
    }
    /**
     * Constructor.
     *
     * @param tableName  The name of the data source associated with this ImmutableAvailability
     * @param dimensionIntervals  The dimension availability map by dimension name
     * @param metricIntervals  The metric availability map
     * @param dimensionDictionary  The dictionary to resolve dimension names against
     */
    public ImmutableAvailability(
            TableName tableName,
            Map<String, Set<Interval>> dimensionIntervals,
            Map<String, Set<Interval>> metricIntervals,
            DimensionDictionary dimensionDictionary
    ) {
        this(tableName, buildAvailabilityMap(dimensionIntervals, metricIntervals, dimensionDictionary));
    }

    /**
     * Constructor.
     *
     * @param tableName  The name of the data source associated with this ImmutableAvailability
     * @param schema  The schema for the availabilities
     * @param dimensionIntervals  The dimension availability map by dimension name
     * @param metricIntervals  The metric availability map
     * @param dimensionDictionary  The dictionary to resolve dimension names against
     */
    public ImmutableAvailability(
            String tableName,
            PhysicalTableSchema schema,
            Map<String, Set<Interval>> dimensionIntervals,
            Map<String, Set<Interval>> metricIntervals,
            DimensionDictionary dimensionDictionary
    ) {
        this(
                TableName.of(tableName),
                buildAvailabilityMap(dimensionIntervals, metricIntervals, dimensionDictionary)
        );
    }

    /**
     * Build an availability map from unbound dimension and metric name maps and dimension dictionaries.
     *
     * @param dimensionIntervals  The dimension availability map by dimension name
     * @param metricIntervals  The metric availability map
     * @param dimensionDictionary  The dictionary to resolve dimension names against
     *
     * @return A map of available intervals by columns
     */
    private static Map<Column, List<Interval>> buildAvailabilityMap(
        Map<String, Set<Interval>> dimensionIntervals,
        Map<String, Set<Interval>> metricIntervals,
        DimensionDictionary dimensionDictionary
    ) {
        Function<Map.Entry<String, Set<Interval>>, Column> dimensionKeyMapper =
                entry -> new DimensionColumn(dimensionDictionary.findByApiName(entry.getKey()));
        Function<Map.Entry<String, Set<Interval>>, Column> metricKeyMapper =
                entry -> new MetricColumn(entry.getKey());
        Function<Map.Entry<String, Set<Interval>>, List<Interval>> valueMapper =
                entry -> new SimplifiedIntervalList(entry.getValue());

        Map<Column, List<Interval>> map = dimensionIntervals.entrySet().stream()
                .collect(Collectors.toMap(dimensionKeyMapper, valueMapper));
        map.putAll(
                metricIntervals.entrySet().stream()
                        .collect(Collectors.toMap(metricKeyMapper, valueMapper))
        );
        return map;
    }

    @Override
    public Set<TableName> getDataSourceNames() {
        return Sets.newHashSet(name);
    }

    @Override
    public List<Interval> get(final Column c) {
        return columnIntervals.get(c);
    }

    @Override
    public Map<Column, List<Interval>> getAvailableIntervals() {
        return columnIntervals;
    }

    @Override
    public int hashCode() {
        return columnIntervals.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ImmutableAvailability) {
            return Objects.equals(
                    columnIntervals,
                    ((ImmutableAvailability) obj).columnIntervals);
        }
        if (obj instanceof Availability) {
            return columnIntervals.equals(((Availability) obj).getAvailableIntervals());
        }
        return false;
    }
}
