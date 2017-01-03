// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.table.Schema;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * Mapper to round floating point values to their ceiling. If a metric is null, the original result is passed along
 * unmodified.
 */
public class SketchRoundUpMapper extends ResultSetMapper implements ColumnMapper {

    String columnName;

    /**
     * Constructor.
     * <p>
     * A SketchRoundUpMapper requires a columnName in order to work, and this constructor defaults the column name to
     * null, meaning that this constructor actually builds an invalid instance that can serve as a template for other
     * instances via withColumnName to set the column name.
     */
    public SketchRoundUpMapper() {
        this.columnName = null;
    }

    /**
     * The column name to round.
     *
     * @param columnName  The column name
     */
    public SketchRoundUpMapper(FieldName columnName) {
        this(columnName.asName());
    }

    /**
     * The column name to round.
     *
     * @param columnName  The column name
     */
    public SketchRoundUpMapper(String columnName) {
        this.columnName = columnName;
    }

    @Override
    protected Result map(Result result, Schema schema) {
        if (columnName == null) {
            throw new IllegalStateException("Cannot map results without a column name");
        }

        MetricColumn metricColumn = (MetricColumn) schema.getColumn(columnName);
        BigDecimal value = result.getMetricValueAsNumber(metricColumn);
        if (value == null) {
            return result;
        }
        BigDecimal newValue = value.setScale(0, RoundingMode.CEILING);
        return result.withMetricValue(metricColumn, newValue);
    }

    @Override
    protected Schema map(Schema schema) {
        return schema;
    }

    @Override
    @Deprecated
    public ResultSetMapper withColumnName(String newColumnName) {
        return new SketchRoundUpMapper(newColumnName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        SketchRoundUpMapper that = (SketchRoundUpMapper) o;
        return super.equals(o) &&
                Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), columnName);
    }
}
