// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.joda.time.Interval;

import java.util.Collection;
import java.util.Collections;

/**
 * Druid timeseries query.
 */
public class TimeSeriesQuery extends AbstractDruidAggregationQuery<TimeSeriesQuery> {

    /**
     * Constructor.
     *
     * @param dataSource  The datasource
     * @param granularity  The granularity
     * @param filter  The filter
     * @param aggregations  The aggregations
     * @param postAggregations  The post-aggregations
     * @param intervals  The intervals
     * @param context  The context
     * @param incrementQueryId  true to fork a new context and bump up the query id, or false to create an exact copy
     * of the context.
     */
    public TimeSeriesQuery(
            DataSource dataSource,
            Granularity granularity,
            Filter filter,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            QueryContext context,
            boolean incrementQueryId
    ) {
        super(
                DefaultQueryType.TIMESERIES,
                dataSource,
                granularity,
                Collections.<Dimension>emptySet(),
                filter,
                aggregations,
                postAggregations,
                intervals,
                context,
                incrementQueryId
        );
    }

    /**
     * Constructor.
     *
     * @param dataSource  The datasource
     * @param granularity  The granularity
     * @param filter  The filter
     * @param aggregations  The aggregations
     * @param postAggregations  The post-aggregations
     * @param intervals  The intervals
     */
    public TimeSeriesQuery(
            DataSource dataSource,
            Granularity granularity,
            Filter filter,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals
    ) {
        this(
                dataSource,
                granularity,
                filter,
                aggregations,
                postAggregations,
                intervals,
                (QueryContext) null,
                false
        );
    }

    //This method is overridden just to redefine its JSON scope
    @JsonIgnore
    @Override
    public Collection<Dimension> getDimensions() {
        return super.getDimensions();
    }

    // CHECKSTYLE:OFF
    @Override
    public TimeSeriesQuery withDataSource(DataSource dataSource) {
        return new TimeSeriesQuery(dataSource, granularity, filter, aggregations, postAggregations, intervals, context, false);
    }

    @Override
    public TimeSeriesQuery withInnermostDataSource(DataSource dataSource) {
        return withDataSource(dataSource);
    }

    @Override
    public TimeSeriesQuery withGranularity(Granularity granularity) {
        return new TimeSeriesQuery(getDataSource(), granularity, filter, aggregations, postAggregations, intervals, context, false);
    }

    @Override
    public TimeSeriesQuery withFilter(Filter filter) {
        return new TimeSeriesQuery(getDataSource(), granularity, filter, aggregations, postAggregations, intervals, context, false);
    }

    @Override
    public TimeSeriesQuery withAggregations(Collection<Aggregation> aggregations) {
        return new TimeSeriesQuery(getDataSource(), granularity, filter, aggregations, postAggregations, intervals, context, false);
    }

    @Override
    public TimeSeriesQuery withPostAggregations(Collection<PostAggregation> postAggregations) {
        return new TimeSeriesQuery(getDataSource(), granularity, filter, aggregations, postAggregations, intervals, context, false);
    }

    @Override
    public TimeSeriesQuery withIntervals(Collection<Interval> intervals) {
        return new TimeSeriesQuery(getDataSource(), granularity, filter, aggregations, postAggregations, intervals, context, true);
    }

    @Override
    public TimeSeriesQuery withAllIntervals(Collection<Interval> intervals) {
        return withIntervals(intervals);
    }

    @Override
    public TimeSeriesQuery withContext(QueryContext context) {
        return new TimeSeriesQuery(getDataSource(), granularity, filter, aggregations, postAggregations, intervals, context, false);
    }

    public GroupByQuery withDimensions(Collection<Dimension> dimensions) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, null, aggregations, postAggregations, intervals, null, context, false);
    }
    // CHECKSTYLE:ON
}
