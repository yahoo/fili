// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.aggregation;

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import java.util.Optional;

/**
 * Provides a mapping from Druid's {@link Aggregation} to a {@link SqlAggregationBuilder}.
 */
public interface DruidSqlAggregationConverter {

    /**
     * Finds the corresponding {@link SqlAggregationBuilder} from a
     * druid aggregation.
     *
     * @param aggregation  The druid aggregation, i.e.
     * {@link com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation}.
     *
     * @return the supported sql aggregation type.
     */
    Optional<SqlAggregationBuilder> fromDruidType(Aggregation aggregation);
}
