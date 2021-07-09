// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.QueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.virtualcolumns.VirtualColumn;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.joda.time.Interval;

import java.util.Collection;

public abstract class AbstractDruidDimensionAggregationQuery
        <Q extends AbstractDruidDimensionAggregationQuery<? super Q>>
        extends AbstractDruidAggregationQuery<Q> implements DruidDimensionAggregationQuery<Q> {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    protected final Having having;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    protected final LimitSpec limitSpec;

    /**
     * Constructor.
     *
     * @param queryType  The type of this query
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
     * @param virtualColumns The virtual columns
     */
    protected AbstractDruidDimensionAggregationQuery(
            QueryType queryType,
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
            Collection<VirtualColumn> virtualColumns
    ) {
        super(
                queryType,
                dataSource,
                granularity,
                dimensions,
                filter,
                aggregations,
                postAggregations,
                intervals,
                context,
                incrementQueryId,
                virtualColumns
        );
        this.having = having;
        this.limitSpec = limitSpec;
    }

    @Override
    public Having getHaving() {
        return having;
    }

    @Override
    public LimitSpec getLimitSpec() {
        return limitSpec;
    }
}
