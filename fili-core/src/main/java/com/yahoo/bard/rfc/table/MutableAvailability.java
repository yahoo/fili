// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.rfc.table;

import com.yahoo.bard.rfc.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.Interval;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MutableAvailability extends HashMap<Column, List<Interval>> implements Availability {

    public MutableAvailability() {
        super();
    }

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


    ImmutableAvailability immutableAvailability() {
        return new ImmutableAvailability(this);
    }
}
