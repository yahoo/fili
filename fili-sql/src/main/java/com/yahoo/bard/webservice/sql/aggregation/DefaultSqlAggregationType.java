// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.aggregation;

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.sql.ApiToFieldMapper;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

/**
 * All the aggregation types supported for use with a sql backend.
 */
public enum DefaultSqlAggregationType {
    SUM("sum", SqlStdOperatorTable.SUM),
    MIN("min", SqlStdOperatorTable.MIN),
    MAX("max", SqlStdOperatorTable.MAX);
    // todo avg

    private final String type;
    private final SqlAggFunction sqlAggFunction;

    /**
     * Construct an DefaultSqlAggregationType with a keyword to look for in a
     * druid aggregation types, i.e. {"longSum", "doubleMin"}.
     *
     * @param type  The keyword to find in a druid type.
     * @param sqlAggFunction  The aggregation function that should be performed.
     */
    DefaultSqlAggregationType(String type, SqlAggFunction sqlAggFunction) {
        this.type = type;
        this.sqlAggFunction = sqlAggFunction;
    }

    /**
     * Gets the type of aggregation.
     *
     * @return the aggregation type.
     */
    public String getType() {
        return type;
    }

    /**
     * Builds an aggregate call using the {@link SqlAggFunction} corresponding
     * to the aggregation type.
     *
     * @param aggregation  The druid aggregation.
     * @param apiToFieldMapper  The mapping between api and physical names for the query.
     *
     * @return the AggCal built from the aggregation type.
     */
    public SqlAggregationBuilder with(Aggregation aggregation, ApiToFieldMapper apiToFieldMapper) {
        return new SqlAggregationBuilder() {
            @Override
            public RelBuilder.AggCall build(RelBuilder builder) {
                return builder.aggregateCall(
                        sqlAggFunction,
                        false,
                        null,
                        apiToFieldMapper.apply(aggregation.getFieldName()),
                        builder.field(apiToFieldMapper.apply(aggregation.getFieldName()))
                );
            }

            @Override
            public Aggregation getAggregation() {
                return aggregation;
            }
        };

    }
}
