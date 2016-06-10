// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.table.Schema;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Mapper to round floating point values to nearest integers.
 */
public class SketchRoundUpMapper extends ResultSetMapper implements ColumnMapper {

    String columnName;

    public SketchRoundUpMapper() {
        this.columnName = null;
    }

    /**
     * The column name to round
     *
     * @param columnName  The column name
     */
    public SketchRoundUpMapper(FieldName columnName) {
        this(columnName.asName());
    }

    /**
     * The column name to round
     *
     * @param columnName  The column name
     */
    public SketchRoundUpMapper(String columnName) {
        this.columnName = columnName;
    }

    @Override
    protected Result map(Result result, Schema schema) {
        MetricColumn metricColumn = (MetricColumn) schema.getColumn(columnName);
        BigDecimal value = result.getMetricValueAsNumber(metricColumn);
        BigDecimal newValue = value.setScale(0, RoundingMode.CEILING);
        return result.withMetricValue(metricColumn, newValue);
    }

    @Override
    protected Schema map(Schema schema) {
        return schema;
    }

    @Override
    public ResultSetMapper withColumnName(String newColumnName) {
        return new SketchRoundUpMapper(newColumnName);
    }

    //CHECKSTYLE:OFF
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        SketchRoundUpMapper that = (SketchRoundUpMapper) o;

        return !(columnName != null ? !columnName.equals(that.columnName) : that.columnName != null);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (columnName != null ? columnName.hashCode() : 0);
        return result;
    }
    //CHECKSTYLE:ON
}
