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
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.druid.model.query.QueryContext;

import org.joda.time.Interval;

import java.util.Collection;

/**
 * Created by hinterlong on 6/22/17.
 */
public class SqlAggregationQuery extends AbstractDruidAggregationQuery<SqlAggregationQuery> {

    public SqlAggregationQuery(DruidAggregationQuery<?> query) {
        super(
                DefaultQueryType.GROUP_BY,
                query.getDataSource(),
                query.getGranularity(),
                query.getDimensions(),
                query.getFilter(),
                query.getAggregations(),
                query.getPostAggregations(),
                query.getIntervals(),
                query.getContext(),
                false
        );
    }

    private SqlAggregationQuery(
            DataSource dataSource,
            Granularity granularity,
            Collection<Dimension> dimensions,
            Filter filter,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            QueryContext context,
            boolean doFork
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
        return new SqlAggregationQuery(dataSource, granularity, dimensions, filter, aggregations, postAggregations, intervals, context, false);
    }

    @Override
    public SqlAggregationQuery withInnermostDataSource(DataSource dataSource) {
        DruidFactQuery<?> innerQuery = (DruidFactQuery<?>) this.dataSource.getQuery();
        return (innerQuery == null) ?
                withDataSource(dataSource) :
                withDataSource(new QueryDataSource(innerQuery.withInnermostDataSource(dataSource)));
    }

    public SqlAggregationQuery withDimensions(Collection<Dimension> dimensions) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context, false);
    }

    @Override
    public SqlAggregationQuery withGranularity(Granularity granularity) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context, false);
    }

    @Override
    public SqlAggregationQuery withFilter(Filter filter) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context, false);
    }

    public SqlAggregationQuery withHaving(Having having) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context, false);
    }

    public SqlAggregationQuery withLimitSpec(LimitSpec limitSpec) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context, false);
    }

    @Override
    public SqlAggregationQuery withAggregations(Collection<Aggregation> aggregations) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context, false);
    }

    @Override
    public SqlAggregationQuery withPostAggregations(Collection<PostAggregation> postAggregations) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context, false);
    }

    @Override
    public SqlAggregationQuery withIntervals(Collection<Interval> intervals) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context, true);
    }

    @Override
    public SqlAggregationQuery withAllIntervals(Collection<Interval> intervals) {
        DruidFactQuery<?> innerQuery = (DruidFactQuery<?>) this.dataSource.getQuery();
        return (innerQuery == null) ?
                withIntervals(intervals) :
                withDataSource(new QueryDataSource(innerQuery.withAllIntervals(intervals))).withIntervals(intervals);
    }

    public SqlAggregationQuery withOrderBy(LimitSpec limitSpec) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context, false);
    }

    @Override
    public SqlAggregationQuery withContext(QueryContext context) {
        return new SqlAggregationQuery(getDataSource(), granularity, dimensions, filter, aggregations, postAggregations, intervals, context, false);
    }
    // CHECKSTYLE:ON
}
