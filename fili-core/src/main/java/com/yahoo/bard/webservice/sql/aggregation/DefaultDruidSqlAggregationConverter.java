// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.aggregation;

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

/**
 * The default implementation of sql aggregations based on druid aggregations.
 */
public class DefaultDruidSqlAggregationConverter implements DruidSqlAggregationConverter {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDruidSqlAggregationConverter.class);

    /**
     * Constructor.
     */
    public DefaultDruidSqlAggregationConverter() {

    }

    @Override
    public Optional<SqlAggregationBuilder> fromDruidType(Aggregation aggregation) {
        String aggregationType = aggregation.getType().toLowerCase(Locale.ENGLISH);
        Optional<SqlAggregationBuilder> sqlAggregationBuilder = Arrays.stream(DefaultSqlAggregationType.values())
                .filter(sqlAggregationType -> aggregationType.contains(sqlAggregationType.type))
                .map(sqlAggregationType -> sqlAggregationType.with(aggregation))
                .findFirst();

        if (!sqlAggregationBuilder.isPresent()) {
            LOG.warn("No Sql Aggregation matches {}", aggregationType);
        }

        return sqlAggregationBuilder;
    }
}
