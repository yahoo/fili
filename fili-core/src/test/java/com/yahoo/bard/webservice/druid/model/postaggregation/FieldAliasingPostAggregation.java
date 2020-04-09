// Copyright 2020 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.WithMetricField;

import java.util.Collections;
import java.util.Set;

/**
 * Test class that represents a (currently non existant) PostAgg that aliases an Aggregation to a different output name.
 */
public class FieldAliasingPostAggregation extends PostAggregation implements WithMetricField {

    private MetricField targetField;

    public FieldAliasingPostAggregation(String outputName, MetricField targetField) {
        super(DefaultPostAggregationType.FIELD_ACCESS, outputName);
        this.targetField = targetField;
    }

    @Override
    public MetricField getMetricField() {
        return targetField;
    }

    @Override
    public WithMetricField withMetricField(MetricField field) {
        return new FieldAliasingPostAggregation(getName(), field);
    }

    @Override
    public PostAggregation withName(String name) {
        return new FieldAliasingPostAggregation(name, getMetricField());
    }

    @Override
    public Set<Dimension> getDependentDimensions() {
        return Collections.emptySet();
    }

    /**
     * Test utility method that actually MUTATES this instance to point to a different field.
     *
     * @param metricField
     */
    public void setTargetMetricField(MetricField metricField) {
        this.targetField = metricField;
    }

    // TODO implement equals, toString, hashCode
}
