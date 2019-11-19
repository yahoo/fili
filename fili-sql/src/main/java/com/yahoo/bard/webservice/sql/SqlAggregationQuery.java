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

import org.joda.time.Interval;

import java.util.Collection;
import java.util.Optional;

/**
 * Wrapper around an {@link DruidAggregationQuery} which always reports
 * itself as a {@link DefaultQueryType#GROUP_BY}.
 */
public class SqlAggregationQuery extends AbstractDruidAggregationQuery<SqlAggregationQuery> {

    /**
     * Wraps a query as a GroupBy Query.
     *
     * @param query  The query to wrap.
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
                query.getContext()
        );
    }

    /**
     * Constructor.
     *
     * @param dataSource  The datasource
     * @param granularity  The granularity
     * @param dimensions  The dimensions
     * @param filter  The filter
     * @param aggregations  The aggregations
     * @param postAggregations  The post-aggregations
     * @param intervals  The intervals
     * @param context  The context
     */
    private SqlAggregationQuery(
            DataSource dataSource,
            Granularity granularity,
            Collection<Dimension> dimensions,
            Filter filter,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            QueryContext context
    ) {
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
                false
        );
    }

    // CHECKSTYLE:OFF
    @Override
    public SqlAggregationQuery withDataSource(DataSource dataSource) {
        return new SqlAggregationQuery(dataSource, granularity, dimensions, filter, aggregations, postAggregations, intervals, context);
    }

    @Override
    public SqlAggregationQuery withInnermostDataSource(DataSource dataSource) {
        Optional<DruidFactQuery<?>> innerQuery = this.dataSource.getQuery().map(DruidFactQuery.class::cast);
        return !innerQuery.isPresent() ?
                withDataSource(dataSource) :
                withDataSource(new QueryDataSource(innerQuery.get().withInnermostDataSource(dataSource)));
    }

    public SqlAggregationQuery withDimensions(Collection<Dimension> dimensions) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context);
    }

    @Override
    public SqlAggregationQuery withGranularity(Granularity granularity) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context);
    }

    @Override
    public SqlAggregationQuery withFilter(Filter filter) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context);
    }

    public SqlAggregationQuery withHaving(Having having) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context);
    }

    public SqlAggregationQuery withLimitSpec(LimitSpec limitSpec) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context);
    }

    @Override
    public SqlAggregationQuery withAggregations(Collection<Aggregation> aggregations) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context);
    }

    @Override
    public SqlAggregationQuery withPostAggregations(Collection<PostAggregation> postAggregations) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context);
    }

    @Override
    public SqlAggregationQuery withIntervals(Collection<Interval> intervals) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context);
    }

    @Override
    public SqlAggregationQuery withAllIntervals(Collection<Interval> intervals) {
        Optional<DruidFactQuery<?>> innerQuery = this.dataSource.getQuery().map(DruidFactQuery.class::cast);
        return !innerQuery.isPresent() ?
                withIntervals(intervals) :
                withDataSource(new QueryDataSource(innerQuery.get().withAllIntervals(intervals))).withIntervals(intervals);
    }

    public SqlAggregationQuery withOrderBy(LimitSpec limitSpec) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context);
    }

    @Override
    public SqlAggregationQuery withContext(QueryContext context) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context);
    }
    // CHECKSTYLE:ON
}
