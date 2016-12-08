// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.metric.MetricColumn;

import com.fasterxml.jackson.databind.JsonNode;

import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A single row of results.
 */
public class Result {

    private final Map<DimensionColumn, DimensionRow> dimensionRows;
    private final Map<MetricColumn, Object> metricValues;
    private final DateTime timeStamp;

    /**
     * Constructor.
     *
     * @param dimensionRows  The dimensions in the result expressed as a map keyed by columns
     * @param metricValues  The metrics in the result expressed as a map keyed by columns
     * @param timeStamp  The timestamp of the result
     */
    public Result(
            Map<DimensionColumn, DimensionRow> dimensionRows,
            Map<MetricColumn, Object> metricValues,
            DateTime timeStamp
    ) {
        this.dimensionRows = Collections.unmodifiableMap(dimensionRows);
        this.metricValues = Collections.unmodifiableMap(metricValues);
        this.timeStamp = timeStamp;
    }

    /**
     * Copy the result, adding or replacing the value of a metric
     * <p>
     * Don't add a without column method because removing columns is far less safe.
     *
     * @param metricColumn  The metric column
     * @param value  The new value for that column
     *
     * @return A new result based on this one
     */
    public Result withMetricValue(MetricColumn metricColumn, Object value) {
        Map<MetricColumn, Object> newMetricValues = new LinkedHashMap<>(metricValues);
        newMetricValues.put(metricColumn, value);
        return new Result(dimensionRows, newMetricValues, timeStamp);
    }

    /**
     * The dimensions in the result row expressed as an unmodifiable map keyed by columns.
     *
     * @return UnmodifiableMap of DimensionColumn to DimensionRow
     */
    public Map<DimensionColumn, DimensionRow> getDimensionRows() {
        return this.dimensionRows;
    }

    /**
     * The metrics in the result row expressed as an unmodifiable map keyed by columns.
     *
     * @return UnmodifiableMap of MetricColumn to Object
     */
    public Map<MetricColumn, Object> getMetricValues() {
        return metricValues;
    }


    /**
     * The timestamp representing the start moment of the aggregated record.
     *
     * @return timestamp
     */
    public DateTime getTimeStamp() {
        return timeStamp;
    }

    /**
     * Fetch a DimensionRow from dimensionValues based on the column provided.
     *
     * @param dimensionColumn  The column whose associated value is desired
     *
     * @return DimensionRow in dimensionValues map corresponding to the column provided as argument
     */
    public DimensionRow getDimensionRow(DimensionColumn dimensionColumn) {
        return this.dimensionRows.get(dimensionColumn);
    }

    /**
     * Fetches the metric value associated with the specified metric column.
     * <p>
     * This method should only be used when the caller does not care about the type of the value returned. If the
     * type is important, either {@link #getMetricValueAsNumber(MetricColumn)},
     * {@link #getMetricValueAsString(MetricColumn)}, {@link #getMetricValueAsBoolean(MetricColumn)} or
     * {@link #getMetricValueAsJsonNode(MetricColumn)} should be used, depending on the desired type.
     *
     * @param column  The metric column whose value is desired
     *
     * @return The value of the specified column as an Object
     */
    public Object getMetricValue(MetricColumn column) {
        return metricValues.get(column);
    }

    /**
     * Fetch the value of the specified numeric metric.
     *
     * @param metricColumn  The metric column whose value is desired
     *
     * @return The value associated to the desired metric as a BigDecimal
     * @throws ClassCastException if the value keyed to metricColumn is not a BigDecimal
     */
    public BigDecimal getMetricValueAsNumber(MetricColumn metricColumn) {
        return (BigDecimal) metricValues.get(metricColumn);
    }

    /**
     * Fetch the String representation of the value of the specified metric.
     *
     * @param metricColumn  The metric column whose value is desired
     *
     * @return The String representation of the value of the desired metric
     */
    public String getMetricValueAsString(MetricColumn metricColumn) {
        return metricValues.containsKey(metricColumn) ? metricValues.get(metricColumn).toString() : null;
    }

    /**
     * Fetch the value of the specified metric as a boolean.
     *
     * @param metricColumn  The metric column whose value is desired
     *
     * @return  The boolean value of the desired metric
     * @throws ClassCastException if the value keyed to metricColumn is not a boolean
     */
    public boolean getMetricValueAsBoolean(MetricColumn metricColumn) {
        return (boolean) metricValues.get(metricColumn);
    }

    /**
     * Fetch the value of the specified metric as a JsonNode.
     *
     * @param metricColumn  The metric column whose value is desired
     *
     * @return The JsonNode associated to the desired metric
     * @throws ClassCastException if the value keyed to metricColumn is not a JsonNode
     */
    public JsonNode getMetricValueAsJsonNode(MetricColumn metricColumn) {
        return (JsonNode) metricValues.get(metricColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dimensionRows, metricValues, timeStamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        Result that = (Result) obj;
        return
                Objects.equals(dimensionRows, that.dimensionRows) &&
                Objects.equals(metricValues, that.metricValues) &&
                Objects.equals(timeStamp, that.timeStamp);
    }

    @Override
    public String toString() {
        return timeStamp.toString() + " " + dimensionRows.keySet() + metricValues.toString();
    }
}
