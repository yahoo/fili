// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Set;

/**
 * This marker interface combines Aggregations and PostAggregations so that they can be collectively referenced.
 * <p>
 * This may become a sub interface of a TBD Column interface to flag all dimensions and metrics.
 */
public interface MetricField {

    /**
     * Get the name of the metric field in the response.
     *
     * @return the name of the metric field in the response
     */
    String getName();

    /**
     * Makes a copy of this MetricField with the current name replaced by the provided name.
     *
     * @param name The new output name for this MetricField
     * @return the updated copy
     */
    MetricField withName(String name);

    /**
     * Indicate if the MetricField is based on a sketch.
     *
     * @return true if the metric field is sketch-based, false otherwise
     */
    boolean isSketch();

    /**
     * Indicate if the MetricField is a floating-point value.
     *
     * @return true if the metric field is a floating-point value, false otherwise.
     */
    boolean isFloatingPoint();

    /**
     * Get the dimensions, if any, this metric depends on.
     *
     * @return A set of dimensions required to satisfy this metric field
     */
    @JsonIgnore
    Set<Dimension> getDependentDimensions();
}
