// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.QueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.joda.time.Interval;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Base class for druid aggregation queries.
 *
 * @param <Q> Type of AbstractDruidAggregationQuery this one extends. This allows the queries to nest their own type.
 */
public abstract class AbstractDruidAggregationQuery<Q extends AbstractDruidAggregationQuery<? super Q>>
        extends AbstractDruidFactQuery<Q> implements DruidAggregationQuery<Q> {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    protected final Collection<Dimension> dimensions;

    // At least one is needed
    protected final Collection<Aggregation> aggregations;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    protected final Collection<PostAggregation> postAggregations;

    /**
     * Constructor.
     *
     * @param queryType  The type of this query
     * @param dataSource  The datasource
     * @param granularity  The granularity
     * @param dimensions  The dimensions
     * @param filter  The filter
     * @param aggregations  The aggregations
     * @param postAggregations  The post-aggregations
     * @param intervals  The intervals
     * @param context  The context
     * @param incrementQueryId  true to fork a new context and bump up the query id, or false to create an exact copy
     * of the context.
     */
    protected AbstractDruidAggregationQuery(
            QueryType queryType,
            DataSource dataSource,
            Granularity granularity,
            Collection<Dimension> dimensions,
            Filter filter,
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            Collection<Interval> intervals,
            QueryContext context,
            boolean incrementQueryId
    ) {
        super(queryType, dataSource, granularity, filter, intervals, context, incrementQueryId);
        this.dimensions = dimensions != null ? Collections.unmodifiableCollection(dimensions) : null;
        this.aggregations = aggregations != null ? new LinkedHashSet<>(aggregations) : null;
        this.postAggregations = postAggregations != null ? new LinkedHashSet<>(postAggregations) : null;
    }

    @Override
    public Collection<Dimension> getDimensions() {
        return dimensions;
    }

    @Override
    public Set<Aggregation> getAggregations() {
        return new LinkedHashSet<>(aggregations);
    }

    @Override
    public Collection<PostAggregation> getPostAggregations() {
        return new LinkedHashSet<>(postAggregations);
    }
}
