// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.orderby.SearchSortDirection;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.joda.time.Interval;

import java.util.Collection;

/**
 * Class model for making Druid search queries.
 */
public class DruidSearchQuery extends AbstractDruidFactQuery<DruidSearchQuery> {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    protected final Collection<Dimension> searchDimensions;

    protected final SearchQuerySpec query;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    protected final SearchSortDirection sort;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    protected final Integer limit;

    /**
     * Constructor.
     *
     * @param dataSource  DataSource for the query
     * @param granularity  Granularity for the query
     * @param filter  Filter for the query
     * @param intervals  Intervals for the query
     * @param searchDimensions  Search Dimensions for the query to search over
     * @param query  Specification for what to search for
     * @param sort  Sort definition for the query
     * @param limit  Limit definition for the query
     * @param context  Query context
     * @param incrementQueryId  Should this query count it's fork
     */
    protected DruidSearchQuery(
            DataSource dataSource,
            Granularity granularity,
            Filter filter,
            Collection<Interval> intervals,
            Collection<Dimension> searchDimensions,
            SearchQuerySpec query,
            SearchSortDirection sort,
            Integer limit,
            QueryContext context,
            boolean incrementQueryId
    ) {
        super(DefaultQueryType.SEARCH, dataSource, granularity, filter, intervals, context, incrementQueryId);
        this.searchDimensions = searchDimensions;
        this.query = query;
        this.sort = sort;
        this.limit = limit;
    }

    /**
     * Constructor.
     * <p>
     * Note that the query constructed here does not count it's forks and has no default query context.
     *
     * @param dataSource  DataSource for the query
     * @param granularity  Granularity for the query
     * @param filter  Filter for the query
     * @param intervals  Intervals for the query
     * @param searchDimensions  Search Dimensions for the query to search over
     * @param query  Specification for what to search for
     * @param sort  Sort definition for the query
     * @param limit  Limit definition for the query
     */
    public DruidSearchQuery(
            DataSource dataSource,
            Granularity granularity,
            Filter filter,
            Collection<Interval> intervals,
            Collection<Dimension> searchDimensions,
            SearchQuerySpec query,
            SearchSortDirection sort,
            Integer limit
    ) {
        this(dataSource, granularity, filter, intervals, searchDimensions, query, sort, limit, null, false);
    }

    public Collection<Dimension> getSearchDimensions() {
        return searchDimensions;
    }

    public SearchQuerySpec getQuery() {
        return query;
    }

    public SearchSortDirection getSort() {
        return sort;
    }

    public Integer getLimit() {
        return limit;
    }

    // CHECKSTYLE:OFF
    @Override
    public DruidSearchQuery withDataSource(DataSource dataSource) {
        return new DruidSearchQuery(dataSource, getGranularity(), getFilter(), getIntervals(), searchDimensions, query, sort, limit, context, false);
    }

    @Override
    public DruidSearchQuery withInnermostDataSource(DataSource dataSource) {
        return withDataSource(dataSource);
    }

    @Override
    public DruidSearchQuery withGranularity(Granularity granularity) {
        return new DruidSearchQuery(getDataSource(), granularity, getFilter(), getIntervals(), searchDimensions, query, sort, limit, context, false);
    }

    @Override
    public DruidSearchQuery withFilter(Filter filter) {
        return new DruidSearchQuery(getDataSource(), getGranularity(), filter, getIntervals(), searchDimensions, query, sort, limit, context, false);
    }

    @Override
    public DruidSearchQuery withIntervals(Collection<Interval> intervals) {
        return new DruidSearchQuery(getDataSource(), getGranularity(), getFilter(), intervals, searchDimensions, query, sort, limit, context, false);
    }

    @Override
    public DruidSearchQuery withAllIntervals(Collection<Interval> intervals) {
        return withIntervals(intervals);
    }

    @Override
    public DruidSearchQuery withContext(QueryContext context) {
        return new DruidSearchQuery(getDataSource(), getGranularity(), getFilter(), getIntervals(), searchDimensions, query, sort, limit, context, false);
    }
    // CHECKSTYLE:ON
}
