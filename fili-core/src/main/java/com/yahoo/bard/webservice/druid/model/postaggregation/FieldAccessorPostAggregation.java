// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.FIELD_ACCESS;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * Model representing lookups of aggregation values.
 */
public class FieldAccessorPostAggregation extends PostAggregation
        implements AggregationReference<FieldAccessorPostAggregation> {

    private static final Logger LOG = LoggerFactory.getLogger(FieldAccessorPostAggregation.class);

    private final Aggregation metricField;

    /**
     * Constructor.
     *
     * @param aggregation  Aggregation to access
     */
    // TODO document HARDENING this contract in changelog!!!!
    public FieldAccessorPostAggregation(@NotNull Aggregation aggregation) {
        super(FIELD_ACCESS, null);

        // Check for null aggregation
        if (aggregation == null) {
            String message = "Aggregation cannot be null";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }

        this.metricField = aggregation;
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
    @JsonIgnore
    public Aggregation getMetricField() {
        return metricField;
    }

    @Override
    public List<Aggregation> getAggregations() {
        return Collections.singletonList(getMetricField());
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
    public FieldAccessorPostAggregation withAggregations(List<Aggregation> aggregations) {
        if (aggregations.isEmpty()) {
            // TODO externalize to interface
            throw new IllegalArgumentException(
                    "Attempted to create FieldAccessorPostAggregation without a dependent aggregation"
            );
        }
        return new FieldAccessorPostAggregation(aggregations.get(0));
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
