// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.Locale;
import java.util.function.Function;

/**
 * All the aggregation types supported for use with a sql backend.
 */
public enum SqlAggregationType {
    SUM("sum"),
    MIN("min"),
    MAX("max");

    private final String type;

    /**
     * Construct an SqlAggregationType with a keyword to look for in a
     * druid aggregation types, i.e. {"longSum", "doubleMin"}.
     *
     * @param type  The keyword to find in a druid type.
     */
    SqlAggregationType(String type) {
        this.type = type;
    }

    /**
     * Finds the corresponding {@link SqlAggregationType} from a
     * druid aggregation type.
     *
     * @param type  The druid aggregation type, i.e. "longSum".
     *
     * @return the supported sql aggregation type.
     */
    public static SqlAggregationType fromDruidType(String type) {
        for (SqlAggregationType a : values()) {
            if (type.toLowerCase(Locale.ENGLISH).contains(a.type)) {
                return a;
            }
        }
        throw new IllegalArgumentException("No corresponding type for " + type);
    }

    /**
     * Builds an aggregate call using the {@link SqlAggFunction} corresponding
     * to the aggregation type.
     *
     * @param aggregation  The druid aggregation.
     * @param builder  The RelBuilder used with calcite to build queries.
     * @param aliasMaker  A function which creates an alias given the field name.
     *
     * @return the AggCal built from the aggregation type.
     */
    public static RelBuilder.AggCall getAggregation(
            Aggregation aggregation,
            RelBuilder builder,
            Function<String, String> aliasMaker
    ) {
        SqlAggFunction aggFunction = null;
        SqlAggregationType type = SqlAggregationType.fromDruidType(aggregation.getType());
        switch (type) {
            case SUM:
                aggFunction = SqlStdOperatorTable.SUM;
                break;
            case MAX:
                aggFunction = SqlStdOperatorTable.MAX;
                break;
            case MIN:
                aggFunction = SqlStdOperatorTable.MIN;
                break;
        }

        String fieldName = aggregation.getFieldName();
        if (aggFunction != null) {
            return builder.aggregateCall(
                    aggFunction,
                    false,
                    null,
                    aliasMaker.apply(fieldName),
                    builder.field(fieldName)
            );
        } else {
            throw new UnsupportedOperationException("No corresponding AggCall for " + type);
        }
    }
}
