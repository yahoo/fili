// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTableSchema;
import com.yahoo.bard.webservice.table.util.ImmutableWrapperMap;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An availability which guarantees immutability on its contents.
 */
public class ImmutableAvailability extends ImmutableWrapperMap<Column, List<Interval>> implements Availability {

    /**
     * Constructor.
     *
     * @param map A map of columns to lists of available intervals
     */
    public ImmutableAvailability(Map<Column, List<Interval>> map) {
        super(map);
    }

    /**
     * Constructor.
     *
     * @param schema  The schema for the availabilities
     * @param dimensionIntervals  The dimension availability map by dimension name
     * @param metricIntervals  The metric availability map
     * @param dimensionDictionary  The dictionary to resolve dimension names against
     */
    public ImmutableAvailability(
            PhysicalTableSchema schema,
            Map<String, Set<Interval>> dimensionIntervals,
            Map<String, Set<Interval>> metricIntervals,
            DimensionDictionary dimensionDictionary
    ) {
        super(buildAvailabilityMap(schema, dimensionIntervals, metricIntervals, dimensionDictionary));
    }

    /**
     * Build an availability map from unbound dimension and metric name maps and dimension dictionaries.
     *
     * @param schema blah blah blah
     * @param dimensionIntervals blah blah blah
     * @param metricIntervals blah blah blah
     * @param dimensionDictionary blah blah blah
     *
     * @return blah blah blah
     */
    private static Map buildAvailabilityMap(
        PhysicalTableSchema schema,
        Map<String, Set<Interval>> dimensionIntervals,
        Map<String, Set<Interval>> metricIntervals,
        DimensionDictionary dimensionDictionary
    ) {
        Function<Entry<String, Set<Interval>>, Column> dimensionKeyMapper =
                entry -> new DimensionColumn(dimensionDictionary.findByApiName(entry.getKey()));
        Function<Entry<String, Set<Interval>>, Column> metricKeyMapper =
                entry -> new MetricColumn(entry.getKey());
        Function<Entry<String, Set<Interval>>, List<Interval>> valueMapper =
                entry -> new SimplifiedIntervalList(entry.getValue());

        Map<Column, List<Interval>> map = dimensionIntervals.entrySet().stream()
                .collect(Collectors.toMap(dimensionKeyMapper, valueMapper));
        map.putAll(
                metricIntervals.entrySet().stream()
                        .collect(Collectors.toMap(metricKeyMapper, valueMapper))
        );
        return map;
    }
}
