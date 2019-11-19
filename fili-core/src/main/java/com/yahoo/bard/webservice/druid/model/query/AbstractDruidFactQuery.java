// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.QueryType;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.filter.Filter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Base class for druid aggregation queries.
 *
 * @param <Q> Type of AbstractDruidAggregationQuery this one extends. This allows the queries to nest their own type.
 */
public abstract class AbstractDruidFactQuery<Q extends AbstractDruidFactQuery<? super Q>> extends AbstractDruidQuery<Q>
        implements DruidFactQuery<Q> {

    protected final Granularity granularity;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    protected final Filter filter;

    // At least one is needed
    protected final Collection<Interval> intervals;

    /**
     * Constructor.
     *
     * @param queryType  The type of this query
     * @param dataSource  The datasource
     * @param granularity  The granularity
     * @param filter  The filter
     * @param intervals  The intervals
     * @param context  The context
     * @param incrementQueryId  true to fork a new context and bump up the query id, or false to create an exact copy
     * of the context.
     */
    protected AbstractDruidFactQuery(
            QueryType queryType,
            DataSource dataSource,
            Granularity granularity,
            Filter filter,
            Collection<Interval> intervals,
            QueryContext context,
            boolean incrementQueryId
    ) {
        super(queryType, dataSource, context, incrementQueryId);
        this.granularity = granularity;
        this.filter = filter;
        this.intervals = Collections.unmodifiableCollection(intervals);
    }

    @Override
    public Granularity getGranularity() {
        return granularity;
    }

    @Override
    public Filter getFilter() {
        return filter;
    }

    @Override
    @JsonSerialize(contentUsing = ToStringSerializer.class)
    public List<Interval> getIntervals() {
        return new ArrayList<>(intervals);
    }
}
