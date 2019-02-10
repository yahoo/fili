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

import com.fasterxml.jackson.annotation.JsonInclude;

import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;

/**
 * Druid groupBy query.
 */
public class GroupByQuery extends AbstractDruidAggregationQuery<GroupByQuery> {

    private static final Logger LOG = LoggerFactory.getLogger(GroupByQuery.class);

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Having having;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final LimitSpec limitSpec;

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
     */
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
                incrementQueryId
        );

        this.having = having;
        this.limitSpec = limitSpec;

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
                        "Context: {}",
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
                this.context
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
                false
        );
    }

    public Having getHaving() {
        return having;
    }

    public LimitSpec getLimitSpec() {
        return limitSpec;
    }

    // CHECKSTYLE:OFF
    @Override
    public GroupByQuery withDataSource(DataSource dataSource) {
        return new GroupByQuery(dataSource, granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false);
    }

    @Override
    public GroupByQuery withInnermostDataSource(DataSource dataSource) {
        Optional<DruidFactQuery<?>> innerQuery = this.dataSource.getQuery().map(DruidFactQuery.class::cast);
        return !innerQuery.isPresent() ?
                withDataSource(dataSource) :
                withDataSource(new QueryDataSource(innerQuery.get().withInnermostDataSource(dataSource)));
    }

    public GroupByQuery withDimensions(Collection<Dimension> dimensions) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false);
    }

    @Override
    public GroupByQuery withGranularity(Granularity granularity) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false);
    }

    @Override
    public GroupByQuery withFilter(Filter filter) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false);
    }

    public GroupByQuery withHaving(Having having) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false);
    }

    public GroupByQuery withLimitSpec(LimitSpec limitSpec) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false);
    }

    @Override
    public GroupByQuery withAggregations(Collection<Aggregation> aggregations) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false);
    }

    @Override
    public GroupByQuery withPostAggregations(Collection<PostAggregation> postAggregations) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false);
    }

    @Override
    public GroupByQuery withIntervals(Collection<Interval> intervals) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, true);
    }

    @Override
    public GroupByQuery withAllIntervals(Collection<Interval> intervals) {
        Optional<DruidFactQuery<?>> innerQuery = this.dataSource.getQuery().map(DruidFactQuery.class::cast);
        return !innerQuery.isPresent() ?
                withIntervals(intervals) :
                withDataSource(new QueryDataSource(innerQuery.get().withAllIntervals(intervals))).withIntervals(intervals);
    }

    public GroupByQuery withOrderBy(LimitSpec limitSpec) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false);
    }

    @Override
    public GroupByQuery withContext(QueryContext context) {
        return new GroupByQuery(getDataSource(), granularity, dimensions, filter, having, aggregations, postAggregations, intervals, limitSpec, context, false);
    }
    // CHECKSTYLE:ON
}
