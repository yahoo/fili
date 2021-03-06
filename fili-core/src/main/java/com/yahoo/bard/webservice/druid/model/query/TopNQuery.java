// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.orderby.TopNMetric;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.virtualcolumns.VirtualColumn;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import org.joda.time.Interval;

import java.util.Collection;
import java.util.Collections;

/**
 * Druid topN query.
 */
public class TopNQuery extends AbstractDruidAggregationQuery<TopNQuery> {

    private final long threshold;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final TopNMetric metric;

    /**
     * Constructor.
     *
     * @param dataSource  The datasource
     * @param granularity  The granularity
     * @param dimension  The dimension
     * @param filter  The filter
     * @param aggregations  The aggregations
     * @param postAggregations  The post-aggregations
     * @param intervals  The intervals
     * @param threshold  The threshold
     * @param metric  The TopN metric
     * @param context  The context
     * @param incrementQueryId  true to fork a new context and bump up the query id, or false to create an exact copy
     * of the context.
     *
     * @deprecated The constructor with virtual columns should be the primary constructor
     */
    @Deprecated
    protected TopNQuery(
            DataSource dataSource,
            Granularity granularity,
            Dimension dimension,
            Filter filter,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            long threshold,
            TopNMetric metric,
            QueryContext context,
            boolean incrementQueryId
    ) {
        this(
                dataSource,
                granularity,
                dimension,
                filter,
                aggregations,
                postAggregations,
                intervals,
                threshold,
                metric,
                context,
                incrementQueryId,
                null
        );
    }

    /**
     * Constructor.
     *
     * @param dataSource  The datasource
     * @param granularity  The granularity
     * @param dimension  The dimension
     * @param filter  The filter
     * @param aggregations  The aggregations
     * @param postAggregations  The post-aggregations
     * @param intervals  The intervals
     * @param threshold  The threshold
     * @param metric  The TopN metric
     *
     * @deprecated The constructor with virtual columns should be used instead
     */
    @Deprecated
    public TopNQuery (
            DataSource dataSource,
            Granularity granularity,
            Dimension dimension,
            Filter filter,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            long threshold,
            TopNMetric metric
    ) {
        this(
                dataSource,
                granularity,
                dimension,
                filter,
                aggregations,
                postAggregations,
                intervals,
                threshold,
                metric,
                null,
                false,
                null
        );
    }

    /**
     * Constructor.
     *
     * @param dataSource  The datasource
     * @param granularity  The granularity
     * @param dimension  The dimension
     * @param filter  The filter
     * @param aggregations  The aggregations
     * @param postAggregations  The post-aggregations
     * @param intervals  The intervals
     * @param threshold  The threshold
     * @param metric  The TopN metric
     * @param virtualColumns The virtual columns
     */
    public TopNQuery (
            DataSource dataSource,
            Granularity granularity,
            Dimension dimension,
            Filter filter,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            long threshold,
            TopNMetric metric,
            Collection<VirtualColumn> virtualColumns
    ) {
        this(
                dataSource,
                granularity,
                dimension,
                filter,
                aggregations,
                postAggregations,
                intervals,
                threshold,
                metric,
                null,
                false,
                virtualColumns
        );
    }

    /**
     * Constructor.
     *
     * @param dataSource  The datasource
     * @param granularity  The granularity
     * @param dimension  The dimension
     * @param filter  The filter
     * @param aggregations  The aggregations
     * @param postAggregations  The post-aggregations
     * @param intervals  The intervals
     * @param threshold  The threshold
     * @param metric  The TopN metric
     * @param context  The context
     * @param incrementQueryId  true to fork a new context and bump up the query id, or false to create an exact copy
     * of the context.
     * @param virtualColumns The virtual columns
     */
    protected TopNQuery(
            DataSource dataSource,
            Granularity granularity,
            Dimension dimension,
            Filter filter,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            long threshold,
            TopNMetric metric,
            QueryContext context,
            boolean incrementQueryId,
            Collection<VirtualColumn> virtualColumns
    ) {
        super(
                DefaultQueryType.TOP_N,
                dataSource,
                granularity,
                Collections.singletonList(dimension),
                filter,
                aggregations,
                postAggregations,
                intervals,
                context,
                incrementQueryId,
                virtualColumns
        );

        this.threshold = threshold;
        this.metric = metric;
    }

    public Dimension getDimension() {
        return dimensions.isEmpty() ? null : dimensions.iterator().next();
    }

    //This method is overridden just to redefine its JSON scope
    @JsonIgnore
    @Override
    public Collection<Dimension> getDimensions() {
        return super.getDimensions();
    }

    public long getThreshold() {
        return threshold;
    }

    public TopNMetric getMetric() {
        return metric;
    }

    // CHECKSTYLE:OFF
    @Override
    public TopNQuery withDataSource(DataSource dataSource) {
        return new TopNQuery(dataSource, granularity, getDimension(), filter, aggregations, postAggregations, intervals, threshold, metric, context, false, virtualColumns);
    }

    @Override
    public TopNQuery withInnermostDataSource(DataSource dataSource) {
        return withDataSource(dataSource);
    }

    public TopNQuery withDimension(Dimension dimension) {
        return new TopNQuery(getDataSource(), granularity, dimension, filter, aggregations, postAggregations, intervals, threshold, metric, context, false, virtualColumns);
    }

    public TopNQuery withMetric(TopNMetric metric) {
        return new TopNQuery(getDataSource(), granularity, getDimension(), filter, aggregations, postAggregations, intervals, threshold, metric, context, false, virtualColumns);
    }

    @Override
    public TopNQuery withGranularity(Granularity granularity) {
        return new TopNQuery(getDataSource(), granularity, getDimension(), filter, aggregations, postAggregations, intervals, threshold, metric, context, false, virtualColumns);
    }

    @Override
    public TopNQuery withFilter(Filter filter) {
        return new TopNQuery(getDataSource(), granularity, getDimension(), filter, aggregations, postAggregations, intervals, threshold, metric, context, false, virtualColumns);
    }

    @Override
    public TopNQuery withAggregations(Collection<Aggregation> aggregations) {
        return new TopNQuery(getDataSource(), granularity, getDimension(), filter, aggregations, postAggregations, intervals, threshold, metric, context, false, virtualColumns);
    }

    @Override
    public TopNQuery withPostAggregations(Collection<PostAggregation> postAggregations) {
        return new TopNQuery(getDataSource(), granularity, getDimension(), filter, aggregations, postAggregations, intervals, threshold, metric, context, false, virtualColumns);
    }

    @Override
    public TopNQuery withIntervals(Collection<Interval> intervals) {
        return new TopNQuery(getDataSource(), granularity, getDimension(), filter, aggregations, postAggregations, intervals, threshold, metric, context, true, virtualColumns);
    }

    @Override
    public TopNQuery withAllIntervals(Collection<Interval> intervals) {
        return withIntervals(intervals);
    }

    @Override
    public TopNQuery withContext(QueryContext context) {
        return new TopNQuery(getDataSource(), granularity, getDimension(), filter, aggregations, postAggregations, intervals, threshold, metric, context, false, virtualColumns);
    }

    @Override
    public TopNQuery withVirtualColumns(Collection<VirtualColumn> virtualColumns) {
        return new TopNQuery(getDataSource(), granularity, getDimension(), filter, aggregations, postAggregations, intervals, threshold, metric, context, false, virtualColumns);
    }
    // CHECKSTYLE:ON
}
