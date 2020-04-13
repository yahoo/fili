// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.MetricField;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * Base type for aggregations.
 */
public abstract class Aggregation implements MetricField {

    private static final Logger LOG = LoggerFactory.getLogger(Aggregation.class);

    private final String name;

    // TODO: Make sure the COUNT aggregation is correctly handled, since it doesn't need a "fieldName"
    private final String fieldName;

    /**
     * Constructor.
     *
     * @param name  Name of the aggregation
     * @param fieldName  Name of the column that this aggregation is aggregating over
     */
    public Aggregation(@NotNull String name, String fieldName) {
        // Check for null name
        if (name == null) {
            String message = "Aggregation name cannot be null";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }

        this.name = name;
        this.fieldName = fieldName;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * The field name for aggregation.
     *
     * @return  The field name for an aggregation.
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * The Type name for the aggregation in the druid configuration.
     *
     * @return the druid 'type' string
     */
    public abstract String getType();

    /**
     * Creates a new Aggregation with the provided name.
     *
     * @param name  Name for the new aggregation
     *
     * @return subclass of Aggregation
     */
    @Override
    public abstract Aggregation withName(String name);

    /**
     * Creates a new Aggregation with the provided field name.
     *
     * @param fieldName  Field name for the new aggregation
     *
     * @return subclass of Aggregation
     */
    public abstract Aggregation withFieldName(String fieldName);

    /**
     * Splits an Aggregation for 2-pass aggregation into an inner &amp; outer Aggregation. The outer aggregation
     * fieldName will reference the inner aggregation name. The inner aggregation is unmodified.
     *
     * @return A pair where pair.left is the outer aggregation and pair.right is the inner.
     */
    public Pair<Optional<Aggregation>, Optional<Aggregation>> nest() {
        String nestingName = this.name;
        Aggregation outer = this.withFieldName(nestingName);
        Aggregation inner = this.withName(nestingName);
        return new ImmutablePair<>(Optional.of(outer), Optional.of(inner));
    }

    @Override
    @JsonIgnore
    public boolean isSketch() {
        return false;
    }

    @Override
    @JsonIgnore
    public boolean isFloatingPoint() {
        return true;
    }

    @Override
    public String toString() {
        return "Aggregation{type=" + getType() + ", name=" + name + ", fieldName=" + fieldName + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof Aggregation)) { return false; }

        Aggregation that = (Aggregation) o;

        return
                Objects.equals(fieldName, that.fieldName) &&
                Objects.equals(name, that.name) &&
                Objects.equals(getType(), that.getType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fieldName, getType());
    }

    @Override
    @JsonIgnore
    public Set<Dimension> getDependentDimensions() {
        return Collections.emptySet();
    }
}
