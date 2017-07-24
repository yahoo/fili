// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.aggregation;

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import org.apache.calcite.tools.RelBuilder;

/**
 * A sql aggregation which is able to translate an {@link Aggregation}
 * into the equivalent {@link RelBuilder.AggCall} using the {@link RelBuilder}.
 */
public interface SqlAggregationType {

    /**
     * Builds an aggregate call using the {@link org.apache.calcite.sql.SqlAggFunction} corresponding
     * to the aggregation type.
     *
     * @param builder  The RelBuilder used with calcite to build the aggregation.
     * @param aggregation  The druid aggregation.
     *
     * @return the AggCall built from the aggregation type.
     */
    RelBuilder.AggCall getAggregation(RelBuilder builder, Aggregation aggregation);
}
