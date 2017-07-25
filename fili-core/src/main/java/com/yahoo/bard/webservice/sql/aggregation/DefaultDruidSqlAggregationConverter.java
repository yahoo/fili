// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.aggregation;

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

/**
 * The default implementation of sql aggregations based on druid aggregations.
 */
public class DefaultDruidSqlAggregationConverter implements DruidSqlAggregationConverter {
    /**
     * Constructor.
     */
    public DefaultDruidSqlAggregationConverter() {

    }

    @Override
    public Optional<SqlAggregationBuilder> fromDruidType(Aggregation aggregation) {
        String aggregationType = aggregation.getType().toLowerCase(Locale.ENGLISH);
        return Arrays.stream(DefaultSqlAggregationType.values())
                .filter(sqlAggregationType -> aggregationType.contains(sqlAggregationType.type))
                .map(sqlAggregationType -> sqlAggregationType.with(aggregation))
                .findFirst();
    }
}
