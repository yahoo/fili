// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.SegmentMetadata;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.availability.MutableAvailability;

import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.validation.constraints.NotNull;

/**
 * An instance of Physical table that is backed by a real remote fact table.
 */
public class ConcretePhysicalTable extends BasePhysicalTable {

    private final Object mutex = new Object();

    private AtomicReference<Availability> availabilityRef = new AtomicReference<>();
    private MutableAvailability workingAvailability;
    private final String factTableName;

    /**
     * Create a concrete physical table.
     *
     * @param name  Fili name of the physical table
     * @param factTableName  Name of the associated table in Druid
     * @param timeGrain  time grain of the table
     * @param columns  The columns for this table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     */
    public ConcretePhysicalTable(
            @NotNull String name,
            @NotNull String factTableName,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Set<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNames
    ) {
        super(name, timeGrain, columns, logicalToPhysicalColumnNames);
        this.factTableName = factTableName;
    }

    /**
     * Create a concrete physical table.
     *
     * @param name  Fili name of the physical table
     * @param timeGrain  time grain of the table
     * @param columns The columns for this table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     */
    public ConcretePhysicalTable(
            @NotNull String name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Set<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNames
    ) {
        this(name, name, timeGrain, columns, logicalToPhysicalColumnNames);
    }

    @Override
    public Availability getAvailability() {
        return availabilityRef.get();
    }

    @Override
    public Availability getWorkingAvailability() {
        return workingAvailability;
    }

    public String getFactTableName() {
        return factTableName;
    }

    /**
     * Add a column to the working intervals.
     *
     * @param columnToAdd  The column instance to add
     * @param intervals  The interval set to add
     *
     * @return True if the workingIntervals had this column already
     *
     * @deprecated manipulate schema before building table
     */
    @Deprecated
    private Boolean addColumn(Column columnToAdd, List<Interval> intervals) {
        return getWorkingAvailability().put(columnToAdd, intervals) == null;
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
            workingAvailability = new MutableAvailability(
                    schema,
                    dimensionIntervals,
                    metricIntervals,
                    dimensionDictionary
            );
            availabilityRef.set(workingAvailability);
        }
    }

    @Override
    public String toString() {
        return super.toString() + " factTableName: " + factTableName + " alignment: " + getTableAlignment();
    }
}
