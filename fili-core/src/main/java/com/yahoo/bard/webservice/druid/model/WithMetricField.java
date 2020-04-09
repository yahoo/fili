package com.yahoo.bard.webservice.druid.model;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Metric that has a dependency on a single different metric field
 */
public interface WithMetricField extends MetricField {


    MetricField getMetricField();

    /**
     * Copy of this MetricField with its dependency pointed at the newly provided field.
     *
     * @param field
     * @return
     */
    WithMetricField withMetricField(MetricField field);

    /**
     * Get the dimensions from all child MetricFields.
     *
     * @return the set of dimensions required to satisfy the fields of all child MetricField
     */
    @Override
    @JsonIgnore
    default Set<Dimension> getDependentDimensions() {
        return getMetricField().getDependentDimensions();
    }
}
