// Copyright 2020 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Set;

/**
 * MetricField that has a dependency on a single different MetricField.
 */
public interface WithMetricField extends MetricField {

    /**
     * Getter for the dependent MetricField.
     *
     * @return the MetricField
     */
    @JsonIgnore
    MetricField getMetricField();

    /**
     * Copy of this MetricField with its dependency pointed at the newly provided field.
     *
     * @param field  The new field to point to
     * @return the copy
     */
    WithMetricField withMetricField(MetricField field);

    @Override
    @JsonIgnore
    default Set<Dimension> getDependentDimensions() {
        return getMetricField().getDependentDimensions();
    }
}
