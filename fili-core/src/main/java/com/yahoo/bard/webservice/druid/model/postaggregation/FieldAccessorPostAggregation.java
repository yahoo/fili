// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.FIELD_ACCESS;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * Model representing lookups of aggregation values.
 */
public class FieldAccessorPostAggregation extends PostAggregation {

    private static final Logger LOG = LoggerFactory.getLogger(FieldAccessorPostAggregation.class);

    private final Aggregation aggregation;

    /**
     * Constructor.
     *
     * @param aggregation  Aggregation to access
     */
    public FieldAccessorPostAggregation(@NotNull Aggregation aggregation) {
        super(FIELD_ACCESS, null);

        // Check for null aggregation
        if (aggregation == null) {
            String message = "Aggregation cannot be null";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }

        this.aggregation = aggregation;
    }

    @Override
    public boolean isSketch() {
        return aggregation.isSketch();
    }

    @JsonIgnore
    public Aggregation getAggregation() {
        return aggregation;
    }

    public String getFieldName() {
        return aggregation.getName();
    }

    @JsonIgnore
    @Override
    public String getName() {
        return null;
    }

    @JsonIgnore
    @Override
    public Set<Dimension> getDependentDimensions() {
        return aggregation.getDependentDimensions();
    }

    @Override
    @JsonIgnore
    public boolean isFloatingPoint() {
        return aggregation.isFloatingPoint();
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

        return aggregation.equals(that.aggregation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), aggregation);
    }
}
