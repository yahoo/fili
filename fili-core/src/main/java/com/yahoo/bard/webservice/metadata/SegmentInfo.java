// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import com.yahoo.bard.webservice.druid.model.metadata.NumberedShardSpec;

import org.joda.time.Interval;

import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;

import java.util.Objects;

/**
 * A class that holds the useful information of a partition of a druid segment in bard.
 */
public class SegmentInfo {
    private final String dataSource;
    private final Interval interval;
    private final String version;
    private final NumberedShardSpec shardSpec;
    private final long size;
    private final String identifier;

    /**
     * Given a druid data segment constructs an object to hold the information of this partition.
     *
     * @param segment  The druid data segments that corresponds to a specific partition of a druid segment.
     */
    public SegmentInfo(DataSegment segment) {
        this.dataSource = segment.getDataSource();
        this.interval = segment.getInterval();
        this.version = segment.getVersion();
        ShardSpec spec = segment.getShardSpec();
        this.shardSpec = spec instanceof NumberedShardSpec ?
                (NumberedShardSpec) spec :
                new NumberedShardSpec((NoneShardSpec) spec);
        this.size = segment.getSize();
        this.identifier = segment.getIdentifier();
    }

    /**
     * Getter for the datasource of this segment partition.
     *
     * @return The datasource.
     */
    public String getDataSource() {
        return dataSource;
    }

    /**
     * Getter for the interval that this segment partition is referring to.
     *
     * @return The interval.
     */
    public Interval getInterval() {
        return interval;
    }

    /**
     * Getter for the version of this segment partition.
     *
     * @return The version.
     */
    public String getVersion() {
        return version;
    }

    /**
     * Getter for the shard spec of this segment partition.
     *
     * @return The shard spec.
     */
    public NumberedShardSpec getShardSpec() {
        return shardSpec;
    }

    /**
     * Getter for the size of this segment partition.
     *
     * @return The shard spec.
     */
    public long getSize() {
        return size;
    }

    /**
     * Getter for the identifier of this segment partition.
     *
     * @return The identifier.
     */
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String toString() {
        return getIdentifier();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (!(o instanceof SegmentInfo)) { return false; }

        final SegmentInfo that = (SegmentInfo) o;

        return
                size == that.size &&
                Objects.equals(identifier, that.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size, identifier);
    }
}
