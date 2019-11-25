// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.aggregation;

import org.apache.calcite.sql.SqlAggFunction;

/**
 * A sql aggregation value object containing all the necessary information to build a sql aggregation.
 */
public class SqlAggregation {
    private final String fieldName;
    private final String name;
    private final SqlAggFunction sqlAggFunction;

    /**
     * Constructor.
     *
     * @param name The api name of the field being aggregated on.
     * @param fieldName  The name of the field being aggregated on.
     * @param sqlAggFunction  The sql aggregation to be done on the field.
     */
    public SqlAggregation(String name, String fieldName, SqlAggFunction sqlAggFunction) {
        this.name = name;
        this.fieldName = fieldName;
        this.sqlAggFunction = sqlAggFunction;
    }

    /**
     * Gets the alias name to be used for the aggregation (i.e. the AS "fieldName") part of a sql aggregation.
     *
     * @return the alias name for the aggregation.
     */
    public String getSqlAggregationAsName() {
        return name;
    }

    /**
     * The field name to call the aggregation on (i.e. the SUM("fieldName")) part of a sql aggregation.
     *
     * @return the field name to aggregate on.
     */
    public String getSqlAggregationFieldName() {
        return fieldName;
    }

    /**
     * Gets the sql aggregation to be used on the field.
     *
     * @return the sql aggregation function.
     */
    public SqlAggFunction getSqlAggFunction() {
        return sqlAggFunction;
    }
}
