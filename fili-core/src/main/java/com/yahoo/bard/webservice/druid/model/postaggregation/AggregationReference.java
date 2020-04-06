// Copyright 2020 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * A PostAggregation that references an Aggregation. While this cannot be fully validated using Java's type system, the
 * behavior of classes that interact with this contract is ONLY defined on PostAggregations.
 *
 * @param <T>  The subtype of PostAggregation that this contract produces.
 */
public interface AggregationReference<T extends PostAggregation> extends MetricField {

    /**
     * Gets the aggregations this PostAggregation depends on.
     *
     * @return the Aggregations
     */
    @JsonIgnore
    List<Aggregation> getAggregations();

    /**
     * Return a copy of this with the dependent aggregations replaced by a COPY of the input aggregations list.
     *
     * @param aggregations  The new aggregations
     * @return a copy of this AggregationReference that points at the provided aggregations
     */
    T withAggregations(List<Aggregation> aggregations);
}
