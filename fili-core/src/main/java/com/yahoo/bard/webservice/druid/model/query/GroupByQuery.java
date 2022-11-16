// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.datasource.QueryDataSource;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.virtualcolumns.VirtualColumn;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;

/**
 * Druid groupBy query.
 */
public class GroupByQuery extends AbstractDruidDimensionAggregationQuery<GroupByQuery> {

    private static final Logger LOG = LoggerFactory.getLogger(GroupByQuery.class);

    /**
     * Constructor.
     *
     * @param dataSource  The datasource
     * @param granularity  The granularity
     * @param dimensions  The dimensions
     * @param filter  The filter
     * @param having  The having clause
     * @param aggregations  The aggregations
     * @param postAggregations  The post-aggregations
     * @param intervals  The intervals
     * @param limitSpec  The limit specification
     * @param context  The context
     * @param incrementQueryId  true to fork a new context and bump up the query id, or false to create an exact copy
     * of the context.
     *
     * @deprecated The constructor with virtual columns should be the primary constructor
     */
    @Deprecated
    protected GroupByQuery(
            DataSource dataSource,
            Granularity granularity,
            Collection<Dimension> dimensions,
            Filter filter,
            Having having,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            LimitSpec limitSpec,
            QueryContext context,
            boolean incrementQueryId
    ) {
        this(
                dataSource,
                granularity,
                dimensions,
                filter,
                having,
                aggregations,
                postAggregations,
                intervals,
                limitSpec,
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
     * @param dimensions  The dimensions
     * @param filter  The filter
     * @param having  The having clause
     * @param aggregations  The aggregations
     * @param postAggregations  The post-aggregations
     * @param intervals  The intervals
     * @param limitSpec  The limit specification
     *
     * @deprecated The constructor with virtual columns should be used instead
     */
    @Deprecated
    public GroupByQuery(
            DataSource dataSource,
            Granularity granularity,
            Collection<Dimension> dimensions,
            Filter filter,
            Having having,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            LimitSpec limitSpec
    ) {
        this(
                dataSource,
                granularity,
                dimensions,
                filter,
                having,
                aggregations,
                postAggregations,
                intervals,
                limitSpec,
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
     * @param dimensions  The dimensions
     * @param filter  The filter
     * @param having  The having clause
     * @param aggregations  The aggregations
     * @param postAggregations  The post-aggregations
     * @param intervals  The intervals
     * @param limitSpec  The limit specification
     * @param virtualColumns The virtual column
     */
    public GroupByQuery(
            DataSource dataSource,
            Granularity granularity,
            Collection<Dimension> dimensions,
            Filter filter,
            Having having,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            LimitSpec limitSpec,
            Collection<VirtualColumn> virtualColumns
    ) {
        this(
                dataSource,
                granularity,
                dimensions,
                filter,
                having,
                aggregations,
                postAggregations,
                intervals,
                limitSpec,
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
     * @param dimensions  The dimensions
     * @param filter  The filter
     * @param having  The having clause
     * @param aggregations  The aggregations
     * @param postAggregations  The post-aggregations
     * @param intervals  The intervals
     * @param limitSpec  The limit specification
     * @param context  The context
     * @param incrementQueryId  true to fork a new context and bump up the query id, or false to create an exact copy
     * of the context.
     * @param virtualColumn The virtual columns
     */
    public GroupByQuery(
            DataSource dataSource,
            Granularity granularity,
            Collection<Dimension> dimensions,
            Filter filter,
            Having having,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            LimitSpec limitSpec,
            QueryContext context,
            boolean incrementQueryId,
            Collection<VirtualColumn> virtualColumn
    ) {
        super(
                DefaultQueryType.GROUP_BY,
                dataSource,
                granularity,
                dimensions,
                filter,
                having,
                aggregations,
                postAggregations,
                intervals,
                limitSpec,
                context,
                incrementQueryId,
                virtualColumn
        );

        LOG.trace(
                "Query type: {}\n\n" +
                        "DataSource: {}\n\n" +
                        "Dimensions: {}\n\n" +
                        "TimeGrain: {}\n\n" +
                        "Filter: {}\n\n" +
                        "Having: {}\n\n" +
                        "Aggregations: {}\n\n" +
                        "Post aggregations: {}\n\n" +
                        "Intervals: {}\n\n" +
                        "Limit spec: {}\n\n" +
                        "Context: {}" +
                        "Virtual columns: {}\n\n",
                this.queryType,
                this.dataSource,
                this.dimensions,
                this.granularity,
                this.filter,
                this.having,
                this.aggregations,
                this.postAggregations,
                this.intervals,
                this.limitSpec,
                this.context,
                this.virtualColumns
        );
    }

    // CHECKSTYLE:OFF
    @Override
    public GroupByQuery withDataSource(DataSource dataSource) {
        return new GroupByQuery(dataSource, granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false, virtualColumns);
    }

    @Override
    public GroupByQuery withInnermostDataSource(DataSource dataSource) {
        Optional<DruidFactQuery<?>> innerQuery = this.dataSource.getQuery().map(DruidFactQuery.class::cast);
        return !innerQuery.isPresent() ?
                withDataSource(dataSource) :
                withDataSource(new QueryDataSource(innerQuery.get().withInnermostDataSource(dataSource)));
    }

    @Override
    public GroupByQuery withDimensions(Collection<Dimension> dimensions) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false, virtualColumns);
    }

    @Override
    public GroupByQuery withGranularity(Granularity granularity) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false, virtualColumns);
    }

    @Override
    public GroupByQuery withFilter(Filter filter) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false, virtualColumns);
    }

    @Override
    public GroupByQuery withHaving(Having having) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false, virtualColumns);
    }

    @Override
    public GroupByQuery withLimitSpec(LimitSpec limitSpec) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false, virtualColumns);
    }

    @Override
    public GroupByQuery withAggregations(Collection<Aggregation> aggregations) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false, virtualColumns);
    }

    @Override
    public GroupByQuery withPostAggregations(Collection<PostAggregation> postAggregations) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false, virtualColumns);
    }

    @Override
    public GroupByQuery withIntervals(Collection<Interval> intervals) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, true, virtualColumns);
    }

    @Override
    public GroupByQuery withAllIntervals(Collection<Interval> intervals) {
        Optional<DruidFactQuery<?>> innerQuery = this.dataSource.getQuery().map(DruidFactQuery.class::cast);
        return !innerQuery.isPresent() ?
                withIntervals(intervals) :
                withDataSource(new QueryDataSource(innerQuery.get().withAllIntervals(intervals))).withIntervals(intervals);
    }

    /**
     * A deprecated alias for the limit spec expression.
     *
     * @param limitSpec  The limit spec predicate.
     *
     * @return A copy of the query
     * @deprecated Use withLimitSpec instead
     */
    @Deprecated
    public GroupByQuery withOrderBy(LimitSpec limitSpec) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false, virtualColumns);
    }

    @Override
    public GroupByQuery withContext(QueryContext context) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false, virtualColumns);
    }

    @Override
    public GroupByQuery withVirtualColumns(Collection<VirtualColumn> virtualColumns) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false, virtualColumns);
    }
    // CHECKSTYLE:ON
}
