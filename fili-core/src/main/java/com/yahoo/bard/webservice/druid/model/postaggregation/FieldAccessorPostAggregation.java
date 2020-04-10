// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.FIELD_ACCESS;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.WithMetricField;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * Model representing a lookup from a different MetricField.
 */
public class FieldAccessorPostAggregation extends PostAggregation implements WithMetricField {

    private static final Logger LOG = LoggerFactory.getLogger(FieldAccessorPostAggregation.class);

    private final MetricField metricField;
    private final List<Aggregation> asAggregation;
    private final List<PostAggregation> asPostAggregation;

    /**
     * Constructor.
     *
     * @param metricField  MetricField to access
     */
    public FieldAccessorPostAggregation(@NotNull MetricField metricField) {
        super(FIELD_ACCESS, null);

        // Check for null aggregation
        if (metricField == null) {
            String message = "Field cannot be null";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }

        this.metricField = metricField;
        this.asAggregation = new ArrayList<>();
        this.asPostAggregation = new ArrayList<>();
        if (metricField instanceof Aggregation) {
            this.asAggregation.add((Aggregation) metricField);
        }
        if (metricField instanceof PostAggregation) {
            this.asPostAggregation.add((PostAggregation) metricField);
        }
    }

    @Override
    public boolean isSketch() {
        return metricField.isSketch();
    }

    /**
     * Retrieve the MetricField column which this field accesses.
     *
     * @return An aggregation or post-aggregation referenced by this post aggregator.
     */
    @Override
    public MetricField getMetricField() {
        return metricField;
    }

    @Override
    public WithMetricField withMetricField(MetricField field) {
        return new FieldAccessorPostAggregation(field);
    }

    public String getFieldName() {
        return metricField.getName();
    }

    @JsonIgnore
    @Override
    public String getName() {
        return null;
    }

    @JsonIgnore
    @Override
    public Set<Dimension> getDependentDimensions() {
        return metricField.getDependentDimensions();
    }

    @Override
    @JsonIgnore
    public boolean isFloatingPoint() {
        return metricField.isFloatingPoint();
    }

    /**
     * Not implemented, since FieldAccessorPostAggregation doesn't take a name.
     *
     * @param name  Not applicable
     *
     * @return nothing. Will throw an IllegalStateException instead
     * @throws IllegalStateException because it does not have a name
     */
    @Override
    public FieldAccessorPostAggregation withName(String name) {
        throw new IllegalStateException("Field Access doesn't take name.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FieldAccessorPostAggregation)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        FieldAccessorPostAggregation that = (FieldAccessorPostAggregation) o;

        return metricField.equals(that.metricField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), metricField);
    }
}
