// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.SegmentMetadata;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.StreamUtils;
import com.yahoo.bard.webservice.util.Utils;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.ReadablePeriod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.validation.constraints.NotNull;

/**
 * Physical Table represents a druid table.
 */
public class PhysicalTable extends Table {
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalTable.class);

    private final AtomicReference<Map<Column, Set<Interval>>> availableIntervalsRef;
    private Map<Column, Set<Interval>> workingIntervals;
    private final Object mutex = new Object();
    private final Map<String, String> logicalToPhysicalColumnNames;
    private final Map<String, String> physicalToLogicalColumnNames;

    /**
     * Create a physical table.
     *
     * @param name  name of the physical table
     * @param timeGrain  time grain of the table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     */
    public PhysicalTable(
            @NotNull String name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Map<String, String> logicalToPhysicalColumnNames
    ) {
        super(name, timeGrain);
        this.availableIntervalsRef = new AtomicReference<>();
        availableIntervalsRef.set(new HashMap<>());
        this.workingIntervals = Collections.synchronizedMap(new HashMap<>());
        this.logicalToPhysicalColumnNames = Collections.unmodifiableMap(logicalToPhysicalColumnNames);
        this.physicalToLogicalColumnNames = Collections.unmodifiableMap(
                this.logicalToPhysicalColumnNames
                        .entrySet()
                        .stream()
                        .collect(StreamUtils.toLinkedMap(Map.Entry::getValue, Map.Entry::getKey))
        );
    }

    @Override
    public Set<Column> getColumns() {
        return getAvailableIntervals().keySet();
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
                String physicalName = nameIntervals.getKey();
                String apiName = getLogicalColumnName(physicalName);
                Dimension dimension = dimensionDictionary.findByApiName(apiName);
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
        Set<Interval> result = getAvailableIntervals().get(new Column(columnName));
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
                IntervalUtils.firstMoment(getAvailableIntervals().values()).orElse(new DateTime())
        );
    }

    /**
     * Determine whether or not this PhysicalTable has a mapping for a specific logical name.
     *
     * @param logicalName  Logical name to check
     * @return True if contains a non-default mapping for the logical name, false otherwise
     */
    public boolean hasLogicalMapping(String logicalName) {
        return logicalToPhysicalColumnNames.containsKey(logicalName);
    }

    /**
     * Translate a logical name into a physical column name. If no translation exists (i.e. they are the same),
     * then the logical name is returned.
     * <p>
     * NOTE: This defaulting behavior <em>WILL BE REMOVED</em> in future releases.
     * <p>
     * The defaulting behavior shouldn't be hit for Dimensions that are serialized via the default serializer and are
     * not properly configured with a logical-to-physical name mapping. Dimensions that are not "normal" dimensions,
     * such as dimensions used for DimensionSpecs in queries to do mapping from fact-level dimensions to something else,
     * should likely use their own serialization strategy so as to not hit this defaulting behavior.
     *
     * @param logicalName  Logical name to lookup in physical table
     * @return Translated logicalName if applicable
     */
    public String getPhysicalColumnName(String logicalName) {
        if (!logicalToPhysicalColumnNames.containsKey(logicalName)) {
            LOG.warn(
                    "No mapping found for logical name '{}' to physical name on table '{}'. Will use logical name as " +
                            "physical name. This is unexpected and should not happen for properly configured " +
                            "dimensions.",
                    logicalName,
                    getName()
            );
        }
        return logicalToPhysicalColumnNames.getOrDefault(logicalName, logicalName);
    }

    /**
     * Translate a physical name into a logical column name. If no translation exists (i.e. they are the same),
     * then the physical name is returned.
     *
     * @param physicalName  Physical name to lookup in physical table
     * @return Translated physicalName if applicable
     */
    private String getLogicalColumnName(String physicalName) {
        return physicalToLogicalColumnNames.getOrDefault(physicalName, physicalName);
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
