// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.rfc.table;

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.SegmentMetadata;
import com.yahoo.bard.webservice.table.Column;

import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.validation.constraints.NotNull;

public class ConcretePhysicalTable extends BasePhysicalTable {

    private final Object mutex = new Object();

    AtomicReference<Availability> availabilityRef;
    MutableAvailability workingAvailability;
    private final String factTableName;

    /**
     * Create a physical table.
     *
     * @param name  Fili name of the physical table
     * @param factTableName  Name of the associated table in Druid
     * @param timeGrain  time grain of the table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     */
    public ConcretePhysicalTable(
            @NotNull String name,
            @NotNull String factTableName,
            @NotNull Set<Column> columns,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Map<String, String> logicalToPhysicalColumnNames
    ) {
        super(name, timeGrain, columns, logicalToPhysicalColumnNames);
        this.factTableName = factTableName;
    }

    @Override
    Availability getAvailability() {
        return availabilityRef.get();
    }

    @Override
    Availability getWorkingAvailability() {
        return workingAvailability;
    }

    public String getFactTableName() {
        return factTableName;
    }

    public boolean addColumn(Column columnToAdd) {
        return schema.add(columnToAdd);
    }

    /**
     * Add a column to the working intervals.
     *
     * @param columnToAdd  The column instance to add
     * @param intervals  The interval set to add
     *
     * @return True if the workingIntervals had this column already
     */
    private Boolean addColumn(Column columnToAdd, List<Interval> intervals) {
        return getWorkingAvailability().put(columnToAdd, intervals) == null;
    }


    public boolean removeColumn(Column columnToRemove) {
        return schema.remove(columnToRemove);
    }

    /**
     * Update the working intervals with values from a map.
     *
     * @param segmentMetadata  A map of names of metrics and sets of intervals over which they are valid
     * @param dimensionDictionary  The dimension dictionary from which to look up dimensions by name
     */
    public synchronized void resetColumns(SegmentMetadata segmentMetadata, DimensionDictionary dimensionDictionary) {
        synchronized (mutex) {
            Map<String, Set<Interval>> dimensionIntervals = segmentMetadata.getDimensionIntervals();
            Map<String, Set<Interval>> metricIntervals = segmentMetadata.getMetricIntervals();
            workingAvailability = new MutableAvailability(schema, dimensionIntervals, metricIntervals, dimensionDictionary);
            commit();
        }
    }

    /**
     * Swaps the actual cache with the built-up temporary cache and creates a fresh, empty temporary cache.
     */
    public synchronized void commit() {
        synchronized (mutex) {
            Availability availability = getWorkingAvailability();
            // workingAvailability = new MutableAvailability
            // availability = new ImmutableAvailability(availability)
        }
    }

    @Override
    public String toString() {
        return super.toString() + " factTableName: " + factTableName + " alignment: " + getTableAlignment();
    }
}
