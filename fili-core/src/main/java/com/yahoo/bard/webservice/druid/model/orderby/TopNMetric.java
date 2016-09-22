// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.orderby;

import com.yahoo.bard.webservice.util.EnumUtils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Optional;

/**
 * TopNMetric class corresponding to the Druid TopNMetricSpec. It specifies how topN values will be sorted.
 */
public class TopNMetric {

    private TopNMetricType type;
    private Optional<String> metricName;
    private Optional<TopNMetric> nested;
    private Optional<String> previousStop;

    /**
     * Types of topN metrics.
     */
    public enum TopNMetricType {
        ALPHA_NUMERIC,
        INVERTED,
        LEXICOGRAPHIC,
        NUMERIC;

        private final String jsonName;

        /**
         * Constructor.
         */
        TopNMetricType() {
            this.jsonName = EnumUtils.enumJsonName(this);
        }

        /**
         * Get the TopNMetricType as a Json value.
         *
         * @return the json representation of the DataSourceType.
         */
        @JsonValue
        public String toJson() {
            return jsonName;
        }
    }

    /**
     * Constructs a numeric sort of the given metric with default sorting order (descending).
     *
     * @param metricName  the metric name
     */
    public TopNMetric(String metricName) {
        this.type = TopNMetricType.NUMERIC;
        this.metricName = Optional.of(metricName);
        this.nested = Optional.empty();
        this.previousStop = Optional.empty();
    }

    /**
     * Constructs a numeric sort of the given metric and explicitly specifies the order (ascending or descending).
     *
     * @param metricName  the metric name
     * @param order  sorting order
     */
    public TopNMetric(String metricName, SortDirection order) {
        this(metricName);
        if (order == SortDirection.ASC) {
            this.type = TopNMetricType.INVERTED;
            this.metricName = Optional.empty();
            this.nested = Optional.of(new TopNMetric(metricName));
        }
    }

    /**
     * Constructs a sort of the given type with a starting point.
     *
     * @param type  the type of sort
     * @param previousStop  the starting point of the sort
     */
    public TopNMetric(TopNMetricType type, String previousStop) {
        this.type = type;
        this.metricName = Optional.empty();
        this.nested = Optional.empty();
        this.previousStop = Optional.of(previousStop);
    }

    /**
     * Constructs a sort of the given type with a starting point and explicitly specifies the order (ascending or
     * descending).
     *
     * @param type  the type of sort
     * @param previousStop  the starting point of the sort
     * @param order  sorting order
     */
    public TopNMetric(TopNMetricType type, String previousStop, SortDirection order) {
        this(type, previousStop);
        if (order == SortDirection.ASC) {
            this.type = TopNMetricType.INVERTED;
            this.nested = Optional.of(new TopNMetric(type, previousStop));
            this.previousStop = Optional.empty();
        }
    }

    /**
     * Getter for the type of TopNMetric.
     *
     * @return type
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public TopNMetricType getType() {
        return type;
    }

    /**
     * Getter for the metric of TopNMetric.
     *
     * @return metric
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Object getMetric() {
        return metricName.isPresent() ? metricName.get() : nested.orElse(null);
    }

    /**
     * Getter for the previousStop of TopNMetric.
     *
     * @return previousStop
     */
    //TODO: use com.fasterxml.jackson.datatype.jdk8 when it becomes more stable for proper Optional serialization
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getPreviousStop() {
        return previousStop.isPresent() ? previousStop.get() : null;
    }
}
