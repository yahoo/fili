// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

import org.joda.time.Interval;

import java.util.Collection;
import java.util.Collections;

/**
 * Druid segment metadata query.
 */
public class SegmentMetadataQuery extends AbstractDruidQuery<SegmentMetadataQuery>
        implements DruidMetadataQuery<SegmentMetadataQuery> {

    private final Collection<Interval> intervals;

    /**
     * Constructor.
     *
     * @param dataSource  The datasource
     * @param intervals  The intervals
     * @param context  The context
     * @param incrementQueryId  true to fork a new context and bump up the query id, or false to create an exact copy
     * of the context.
     */
    protected SegmentMetadataQuery(
            DataSource dataSource,
            Collection<Interval> intervals,
            QueryContext context,
            boolean incrementQueryId
    ) {
        super(DefaultQueryType.SEGMENT_METADATA, dataSource, context, incrementQueryId);
        this.intervals = Collections.unmodifiableCollection(intervals);
    }

    /**
     * Constructor.
     *
     * @param dataSource  The datasource
     * @param intervals  The intervals
     */
    public SegmentMetadataQuery(DataSource dataSource, Collection<Interval> intervals) {
        this(dataSource, intervals, null, false);
    }

    @JsonSerialize(contentUsing = ToStringSerializer.class)
    public Collection<Interval> getIntervals() {
        return intervals;
    }

    @Override
    public SegmentMetadataQuery withDataSource(DataSource dataSource) {
        return new SegmentMetadataQuery(dataSource, getIntervals());
    }

    @Override
    public SegmentMetadataQuery withInnermostDataSource(DataSource dataSource) {
        return withDataSource(dataSource);
    }

    @Override
    public SegmentMetadataQuery withContext(QueryContext context) {
        return new SegmentMetadataQuery(getDataSource(), getIntervals(), context, false);
    }

    /**
     * Returns a copy of this query with the specified intervals.
     *
     * @param intervals  the new intervals
     *
     * @return the query copy
     */
    public SegmentMetadataQuery withIntervals(Collection<Interval> intervals) {
        return new SegmentMetadataQuery(getDataSource(), intervals);
    }
}
