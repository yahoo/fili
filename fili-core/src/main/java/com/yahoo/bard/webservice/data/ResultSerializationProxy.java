// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Simplified version of Result class for json format serialization.
 */
public class ResultSerializationProxy {

    public static final String DIMENSION_VALUES_KEY = "dimensionValues";
    public static final String METRIC_VALUES_KEY = "metricValues";
    public static final String TIMESTAMP_KEY = "timeStamp";

    private final Result result;
    private final Map<String, String> dimensionValues;
    private final Map<String, Object> metricValues;
    private final String timeStamp;

    /**
     * Constructor.
     *
     * @param result  Result row from a result set
     */
    public ResultSerializationProxy(Result result) {
        this.result = result;
        this.dimensionValues = getDimensionValues(result);
        this.metricValues = getMetricValues(result);
        this.timeStamp = result.getTimeStamp().toString();
    }

    @JsonProperty(DIMENSION_VALUES_KEY)
    public Map<String, String> getDimensionValues() {
        return dimensionValues;
    }

    @JsonProperty(METRIC_VALUES_KEY)
    public Map<String, Object> getMetricValues() {
        return metricValues;
    }

    @JsonProperty(TIMESTAMP_KEY)
    public String getTimeStamp() {
        return timeStamp;
    }

    /**
     * Get type of class type of metric values mapped with metric names.
     *
     * @return Map of metric names and its value type
     */
    @JsonIgnore
    public Map<String, String> getMetricValuesType() {
        return result.getMetricValues().entrySet().stream()
                .filter(metricValue -> metricValue != null)
                .collect(Collectors.toMap(k -> k.getKey().getName(), v -> getType(v.getValue())));
    }

    /**
     * Get type of value for a given metric value.
     *
     * @param metricValue  Metric value to find the type
     *
     * @return Type of value. ex: "java.math.BigDecimal" by default it is "java.lang.String"
     */
    private String getType(Object metricValue) {
        return metricValue == null ? String.class.getName() : metricValue.getClass().getName();
    }

    /**
     * Generates the dimension names and its unique id map from Result for custom serialization.
     *
     * @param result  Result object for the custom serialization
     *
     * @return custom map of dimension names and their respective unique id
     */
    private Map<String, String> getDimensionValues(Result result) {
        return result.getDimensionRows().entrySet().stream().collect(Collectors.toMap(
                columnRow -> columnRow.getKey().getName(),
                columnRow -> columnRow.getValue().get(columnRow.getKey().getDimension().getKey())
        ));
    }

    /**
     * Generates the metric names and their value map from Result for custom serialization.
     *
     * @param result  Result object for the custom serialization
     *
     * @return  custom map of metric names and their values
     */
    private Map<String, Object> getMetricValues(Result result) {
        return result.getMetricValues().entrySet().stream().collect(Collectors.toMap(
                metricColumn -> metricColumn.getKey().getName(),
                metricColumn -> metricColumn.getValue()
        ));
    }
}
