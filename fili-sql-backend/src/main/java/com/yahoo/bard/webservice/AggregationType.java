// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.InputMismatchException;

/**
 * Created by hinterlong on 6/6/17.
 */
public enum AggregationType {
    SUM("sum"),
    MIN("min"),
    MAX("max");

    private final String type;

    AggregationType(String type) {
        this.type = type;
    }

    public static AggregationType fromDruidType(String type) {
        for (AggregationType a : values()) {
            if (type.toLowerCase().contains(a.type)) {
                return a;
            }
        }
        throw new IllegalArgumentException("No corresponding type for " + type);
    }

    public static RelBuilder.AggCall getAggregation(
            AggregationType a,
            RelBuilder builder,
            String alias,
            String fieldName
    ) {
        SqlAggFunction aggFunction = null;
        switch (a) {
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

        if (aggFunction != null) {
            return builder.aggregateCall(aggFunction, false, null, alias + fieldName, builder.field(fieldName));
        } else {
            throw new UnsupportedOperationException("No corresponding AggCall for " + a);
        }
    }
}
