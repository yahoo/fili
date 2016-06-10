// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Set;

/**
 * This marker interface combines Aggregations and PostAggregations so that they can be collectively referenced.
 *
 * This may become a sub interface of a TBD Column interface to flag all dimensions and metrics.
 */
public interface MetricField {

    String getName();

    // TODO: Doc me
    boolean isSketch();

    // TODO: Doc me
    boolean isFloatingPoint();

    /**
     * Get the dimensions, if any, this metric depends on
     *
     * @return A set of dimensions required to satisfy this metric field
     */
    @JsonIgnore
    Set<Dimension> getDependentDimensions();
}
