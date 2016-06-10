// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import com.yahoo.bard.webservice.table.Column;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Represents a druid segment
 */
public class DruidSegment {

    private final String id;
    private final DateTime timeStamp;
    private final Interval interval;
    private Set<Integer> partitions;
    private Set<Column> columns;

    public DruidSegment(String id, DateTime timeStamp, Interval interval, int partition, Set<Column> columns) {
        this.id = id;
        this.timeStamp = timeStamp;
        this.interval = interval;
        this.partitions = new LinkedHashSet<>(Arrays.asList(partition));
        this.columns = new LinkedHashSet<>(columns);
    }

    public String getId() {
        return id;
    }

    public DateTime getTimeStamp() {
        return timeStamp;
    }

    public Interval getInterval() {
        return interval;
    }

    public Set<Integer> getPartitions() {
        return new LinkedHashSet<>(partitions);
    }

    public Set<Column> getColumns() {
        return new LinkedHashSet<>(columns);
    }

    /**
     * Add a partition number to partitions
     *
     * @param partition  partition number
     */
    public void addPartition(int partition) {
        partitions.add(partition);
    }

    public void setColumns(Set<Column> columns) {
        this.columns = columns;
    }
}
