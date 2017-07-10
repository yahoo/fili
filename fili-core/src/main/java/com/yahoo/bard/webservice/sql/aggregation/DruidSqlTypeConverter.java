// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.aggregation;

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import java.util.Optional;

/**
 * Provides a mapping from Druid's {@link Aggregation} to a {@link SqlAggregationType}.
 */
public interface DruidSqlTypeConverter {

    /**
     * Finds the corresponding {@link SqlAggregationType} from a
     * druid aggregation type.
     *
     * @param type  The druid aggregation type, i.e. "longSum".
     *
     * @return the supported sql aggregation type.
     */
    Optional<SqlAggregationType> fromDruidType(String type);

    /**
     * Finds the corresponding {@link SqlAggregationType} from a
     * druid aggregation. Default implementation is backed by {@link #fromDruidType(String)}.
     *
     * @param aggregation  The druid aggregation, i.e.
     * {@link com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation}.
     *
     * @return the supported sql aggregation type.
     */
    default Optional<SqlAggregationType> fromDruidType(Aggregation aggregation) {
        return fromDruidType(aggregation.getType());
    }
}
