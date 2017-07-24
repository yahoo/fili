// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.aggregation;

import java.util.Locale;
import java.util.Optional;

/**
 * The default implementation of sql aggregations based on druid aggregations.
 */
public class DefaultDruidSqlTypeConverter implements DruidSqlTypeConverter {

    /**
     * Constructor.
     */
    public DefaultDruidSqlTypeConverter() {

    }

    @Override
    public Optional<SqlAggregationType> fromDruidType(String type) {
        for (DefaultSqlAggregationType aggregationType : DefaultSqlAggregationType.values()) {
            if (type.toLowerCase(Locale.ENGLISH).contains(aggregationType.type)) {
                return Optional.of(aggregationType);
            }
        }
        return Optional.empty();
    }
}
