// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.datasource.QueryDataSource;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.query.AbstractDruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidFactQuery;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.query.QueryContext;
import com.yahoo.bard.webservice.druid.model.virtualcolumns.VirtualColumn;

import org.joda.time.Interval;

import java.util.Collection;

/**
 * Wrapper around an {@link DruidAggregationQuery} which always reports
 * itself as a {@link DefaultQueryType#GROUP_BY}.
 */
public class SqlAggregationQuery extends AbstractDruidAggregationQuery<SqlAggregationQuery> {

    /**
     * Wraps a query as a GroupBy Query.
     *
     * @param query The query to wrap.
     */
    public SqlAggregationQuery(DruidAggregationQuery<?> query) {
        this(
                query.getDataSource(),
                query.getGranularity(),
                query.getDimensions(),
                query.getFilter(),
                query.getAggregations(),
                query.getPostAggregations(),
                query.getIntervals(),
                query.getContext(),
                query.getVirtualColumns());
    }

    /**
     * Constructor.
     *
     * @param dataSource The datasource
     * @param granularity The granularity
     * @param dimensions The dimensions
     * @param filter The filter
     * @param aggregations The aggregations
     * @param postAggregations The post-aggregations
     * @param intervals The intervals
     * @param context The context
     *
     * @deprecated The constructor with virtual columns should be the primary constructor
     */
    @Deprecated
    private SqlAggregationQuery(
            DataSource dataSource,
            Granularity granularity,
            Collection<Dimension> dimensions,
            Filter filter,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            QueryContext context) {
        this(
                dataSource,
                granularity,
                dimensions,
                filter,
                aggregations,
                postAggregations,
                intervals,
                context,
                null);
    }

    /**
     * Constructor.
     *
     * @param dataSource The datasource
     * @param granularity The granularity
     * @param dimensions The dimensions
     * @param filter The filter
     * @param aggregations The aggregations
     * @param postAggregations The post-aggregations
     * @param intervals The intervals
     * @param context The context
     * @param virtualColumns The virtual columns
     */
    private SqlAggregationQuery(
            DataSource dataSource,
            Granularity granularity,
            Collection<Dimension> dimensions,
            Filter filter,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            QueryContext context,
            Collection<VirtualColumn> virtualColumns) {
        super(
                DefaultQueryType.GROUP_BY,
                dataSource,
                granularity,
                dimensions,
                filter,
                aggregations,
                postAggregations,
                intervals,
                context,
                false,
                null);
    }

    // CHECKSTYLE:OFF
    @Override
    public SqlAggregationQuery withDataSource(DataSource dataSource) {
        return new SqlAggregationQuery(dataSource, granularity, dimensions, filter, aggregations, postAggregations,
                intervals, context, virtualColumns);
    }

    @Override
    public SqlAggregationQuery withInnermostDataSource(DataSource dataSource) {
        return withDataSource(this.dataSource.getQuery()
                .map(DruidFactQuery.class::cast)
                .map(innerQuery -> (DataSource) innerQuery.withInnermostDataSource(dataSource))
                .orElse(dataSource));
    }

    public SqlAggregationQuery withDimensions(Collection<Dimension> dimensions) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations,
                intervals, context, virtualColumns);
    }

    @Override
    public SqlAggregationQuery withGranularity(Granularity granularity) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations,
                intervals, context, virtualColumns);
    }

    @Override
    public SqlAggregationQuery withFilter(Filter filter) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations,
                intervals, context, virtualColumns);
    }

    public SqlAggregationQuery withHaving(Having having) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations,
                intervals, context, virtualColumns);
    }

    public SqlAggregationQuery withLimitSpec(LimitSpec limitSpec) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations,
                intervals, context, virtualColumns);
    }

    @Override
    public SqlAggregationQuery withAggregations(Collection<Aggregation> aggregations) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations,
                intervals, context, virtualColumns);
    }

    @Override
    public SqlAggregationQuery withPostAggregations(Collection<PostAggregation> postAggregations) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations,
                intervals, context, virtualColumns);
    }

    @Override
    public SqlAggregationQuery withIntervals(Collection<Interval> intervals) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations,
                intervals, context, virtualColumns);
    }

    @Override
    public SqlAggregationQuery withAllIntervals(Collection<Interval> intervals) {
        return this.dataSource.getQuery()
                .map(DruidFactQuery.class::cast)
                .map(innerQuery -> withDataSource(new QueryDataSource(innerQuery.withAllIntervals(intervals)))
                        .withIntervals(intervals))
                .orElseGet(() -> withIntervals(intervals));
    }

    public SqlAggregationQuery withOrderBy(LimitSpec limitSpec) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations,
                intervals, context, virtualColumns);
    }

    @Override
    public SqlAggregationQuery withContext(QueryContext context) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations,
                intervals, context, virtualColumns);
    }

    @Override
    public SqlAggregationQuery withVirtualColumns(Collection<VirtualColumn> virtualColumns) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations,
                intervals, context, virtualColumns);
    }
    // CHECKSTYLE:ON
}
