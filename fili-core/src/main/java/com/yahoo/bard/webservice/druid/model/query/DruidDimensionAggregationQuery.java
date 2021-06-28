// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec;

import java.util.Collection;

public interface DruidDimensionAggregationQuery<Q extends DruidDimensionAggregationQuery<? super Q>>
        extends DruidAggregationQuery<Q> {

    /**
     * Returns the Having (post aggregation filters) of the query, if any.
     *
     * @return the Having filter
     */
    Having getHaving();

    /**
     * Returns the sorting and truncating rules of the query, if any.
     *
     * @return the query aggregations
     */
    LimitSpec getLimitSpec();

    /**
     * Return a copy of the query with the grouping dimensions changed.
     *
     * @param dimensions The grouping dimensions
     *
     * @return a copy of the query
     */
    Q withDimensions(Collection<Dimension> dimensions);

    /**
     * Return a copy of the query with the Having predicate changed.
     *
     * @param having The having predicate
     *
     * @return a copy of the query
     */
    Q withHaving(Having having);

    /**
     * Return a copy of the query with the LimitSpec changed.
     *
     * @param limitSpec The limitSpec definition
     *
     * @return a copy of the query
     */
    Q withLimitSpec(LimitSpec limitSpec);
}
