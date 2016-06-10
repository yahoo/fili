// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;
import com.yahoo.bard.webservice.metadata.SegmentMetadata;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.Utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.ReadablePeriod;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.validation.constraints.NotNull;

/**
 * Physical Table represents a druid table
 */
public class PhysicalTable extends Table {

    private final AtomicReference<Map<Column, Set<Interval>>> availableIntervalsRef;
    private Map<Column, Set<Interval>> workingIntervals;
    private final Object mutex = new Object();

    /**
     * Create a physical table.
     *
     * @param name  name of the physical table
     * @param timeGrain  time grain of the table
     */
    public PhysicalTable(@NotNull String name, @NotNull ZonedTimeGrain timeGrain) {
        super(name, timeGrain);
        this.availableIntervalsRef = new AtomicReference<>();
        availableIntervalsRef.set(new HashMap<>());
        this.workingIntervals = Collections.synchronizedMap(new HashMap<>());
    }

    /**
     * Create a physical table.
     *
     * @param name  name of the physical table
     * @param timeGrain  time grain of the table (defaulted to UTC)
     *
     * @deprecated ZonedTimeZones should be used for physical tables
     */
    @Deprecated
    public PhysicalTable(@NotNull String name, @NotNull ZonelessTimeGrain timeGrain) {
        this(name, new ZonedTimeGrain(timeGrain, DateTimeZone.UTC));
    }

    @Override
    public Set<Column> getColumns() {
        return availableIntervalsRef.get().keySet();
    }

    @Override
    public Boolean addColumn(Column columnToAdd) {
        synchronized (mutex) {
            return addColumn(columnToAdd, Collections.<Interval>emptySet());
        }
    }

    /**
     * Get the time grain from granularity.
     *
     * @return The time grain of this physical table
     */
    public ZonedTimeGrain getTimeGrain() {
        return (ZonedTimeGrain) getGranularity();
    }

    /**
     * Add a column to the working intervals.
     *
     * @param columnToAdd  The column instance to add
     * @param intervals  The interval set to add
     *
     * @return True if the workingIntervals had this column already
     */
    private Boolean addColumn(Column columnToAdd, Set<Interval> intervals) {
        return workingIntervals.put(columnToAdd, intervals) == null;
    }

    @Override
    public Boolean removeColumn(Column columnToRemove) {
        synchronized (mutex) {
            return workingIntervals.remove(columnToRemove) == null;
        }
    }

    /**
     * Getter for active column intervals.
     *
     * @return tableEntries map of column to set of available intervals
     */
    public Map<Column, Set<Interval>> getAvailableIntervals() {
        return availableIntervalsRef.get();
    }

    /**
     * Getter for working copy of the column intervals.
     *
     * @return tableEntries map of column to set of available intervals
     */
    public Map<Column, Set<Interval>> getWorkingIntervals() {
        return Utils.makeImmutable(workingIntervals);
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

            workingIntervals.clear();
            for (Map.Entry<String, Set<Interval>> nameIntervals : dimensionIntervals.entrySet()) {
                Dimension dimension = dimensionDictionary.findByDruidName(nameIntervals.getKey());
                // Schema evolution may lead to unknown dimensions, skip these
                if (dimension == null) {
                    continue;
                }
                DimensionColumn dimensionColumn = DimensionColumn.addNewDimensionColumn(this, dimension);
                workingIntervals.put(dimensionColumn, nameIntervals.getValue());
            }
            for (Map.Entry<String, Set<Interval>> nameIntervals : metricIntervals.entrySet()) {
                MetricColumn metricColumn = MetricColumn.addNewMetricColumn(this, nameIntervals.getKey());
                workingIntervals.put(metricColumn, nameIntervals.getValue());
            }
            commit();
        }
    }

    /**
     * Swaps the actual cache with the built-up temporary cache and creates a fresh, empty temporary cache.
     */
    public synchronized void commit() {
        synchronized (mutex) {
            Map<Column, Set<Interval>> temp = workingIntervals;
            workingIntervals = Collections.synchronizedMap(new LinkedHashMap<>());
            availableIntervalsRef.set(Collections.unmodifiableMap(new LinkedHashMap<>(temp)));
            super.columns = new LinkedHashSet<>(temp.keySet());
        }
    }

    /**
     * Fetch a set of intervals given a column name.
     *
     * @param columnName  Name of the column
     *
     * @return Set of intervals associated with a column, empty if column is missing
     */
    public Set<Interval> getIntervalsByColumnName(String columnName) {
        Set<Interval> result = availableIntervalsRef.get().get(new Column(columnName));
        if (result != null) {
            return result;
        }
        return Collections.emptySet();
    }

    /**
     * Get a date time that the table will align to based on grain and available intervals.
     *
     * @return The time of either the first available interval of any columns in this table or now, floored to the
     * table's time grain.
     */
    public DateTime getTableAlignment() {
        return getTimeGrain().roundFloor(
                IntervalUtils.firstMoment(availableIntervalsRef.get().values()).orElse(new DateTime())
        );
    }

    /**
     * Get the table bucketing as a period.
     *
     * @return The table bucketing as a period
     */
    public ReadablePeriod getTablePeriod() {
        return getTimeGrain().getPeriod();
    }

    @Override
    public String toString() {
        return super.toString() + " alignment: " + getTableAlignment();
    }
}
