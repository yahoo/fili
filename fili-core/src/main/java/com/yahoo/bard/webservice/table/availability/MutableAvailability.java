// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.table.PhysicalTableSchema;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.Interval;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An availability which accepts updates to it's columns and their intervals.
 */
public class MutableAvailability extends HashMap<Column, List<Interval>> implements Availability {

    /**
     * Constructor.
     *
     * @param schema  The schema for the availabilities
     * @param dimensionIntervals  The dimension availability map by dimension name
     * @param metricIntervals  The metric availability map
     * @param dimensionDictionary  The dictionary to resolve dimension names against
     */
    public MutableAvailability(
            PhysicalTableSchema schema,
            Map<String, Set<Interval>> dimensionIntervals,
            Map<String, Set<Interval>> metricIntervals,
            DimensionDictionary dimensionDictionary
    ) {
        for (Map.Entry<String, Set<Interval>> nameIntervals : dimensionIntervals.entrySet()) {
            String physicalName = nameIntervals.getKey();
            schema.getLogicalColumnNames(physicalName).stream()
                    .map(dimensionDictionary::findByApiName)
                    .filter(Objects::nonNull)
                    .forEach(
                            dimension -> {
                                DimensionColumn dimensionColumn  = new DimensionColumn(dimension);
                                put(dimensionColumn, new SimplifiedIntervalList(nameIntervals.getValue()));
                            }
                    );
        }
        for (Map.Entry<String, Set<Interval>> nameIntervals : metricIntervals.entrySet()) {
            MetricColumn metricColumn = new MetricColumn(nameIntervals.getKey());
            put(metricColumn, new SimplifiedIntervalList(nameIntervals.getValue()));
        }
    }

    /**
     * Make an immutable copy of this availability.
     *
     * @return An immutable copy
     */
    ImmutableAvailability immutableAvailability() {
        return new ImmutableAvailability(this);
    }
}
