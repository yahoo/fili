// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.filter.Filter;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.joda.time.Interval;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Common interface for Druid Fact Query classes.
 *
 * @param <Q> class that implements DruidFactQuery
 */
public interface DruidFactQuery<Q extends DruidFactQuery<? super Q>> extends DruidQuery<Q> {

    /**
     * Returns the time grain (granularity) of the query.
     *
     * @return the query time grain
     */
    Granularity getGranularity();

    /**
     * Returns the filter object of the query.
     *
     * @return the query filter object
     */
    Filter getFilter();

    /**
     * Returns the intervals of the query.
     *
     * @return the query intervals
     */
    List<Interval> getIntervals();

    /**
     * Returns a copy of this query with the specified time grain.
     *
     * @param granularity  the new time grain
     *
     * @return the query copy
     */
    Q withGranularity(Granularity granularity);

    /**
     * Returns a copy of this query with the specified filter.
     *
     * @param filter  the new filter
     *
     * @return the query copy
     */
    Q withFilter(Filter filter);

    /**
     * Returns a copy of this query with the specified intervals.
     *
     * @param intervals  the new intervals
     *
     * @return the query copy
     */
    Q withIntervals(Collection<Interval> intervals);

    /**
     * Returns a copy of this query with the specified intervals set in this and all inner queries.
     * <p>
     * All nested queries are copies of themselves with the specified intervals set.
     *
     * @param intervals  the new intervals
     *
     * @return the query copy
     */
    Q withAllIntervals(Collection<Interval> intervals);

    @Override
    @JsonIgnore
    default Optional<? extends DruidFactQuery> getInnerQuery() {
        return DruidQuery.super.getInnerQuery().map(DruidFactQuery.class::cast);
    }

    @Override
    @JsonIgnore
    default DruidFactQuery<?> getInnermostQuery() {
        return (DruidFactQuery<?>) DruidQuery.super.getInnermostQuery();
    }
}
