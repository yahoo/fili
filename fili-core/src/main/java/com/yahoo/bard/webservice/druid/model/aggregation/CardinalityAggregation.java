// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.serializers.DimensionToNameSerializer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Creates a Druid cardinality aggregation.
 * <p>
 * Cardinality aggregators target dimension columns and not metric columns in their underlying data source, counting
 * either dimension cardinality sums, or sums of dimension tuples
 * <p>
 * Does not support DimensionSpecs, only accept Strings as fieldNames
 */
public class CardinalityAggregation extends Aggregation {

    @JsonProperty(value = "fieldNames")
    @JsonSerialize(contentUsing = DimensionToNameSerializer.class)
    private final Set<Dimension> dimensions;
    private final boolean byRow;

    /**
     * Constructor.
     *
     * @param name  Name of the field
     * @param dimensions  The dimensions whose cardinality you want to capture
     * @param byRow  Whether to use Row or Value cardinality
     */
    public CardinalityAggregation(String name, Set<Dimension> dimensions, boolean byRow) {
        super(name, "");
        this.dimensions = Collections.unmodifiableSet(new LinkedHashSet<>(dimensions));
        this.byRow = byRow;
    }

    public boolean isByRow() {
        return byRow;
    }

    /**
     * Field name is not part of the Cardinality aggregation model in druid.
     * Suppress this field.
     *
     * @return empty string which denotes no underlying metric field
     */
    @JsonIgnore
    @Override
    public String getFieldName() {
        return "";
    }

    @Override
    public String getType() {
        return "cardinality";
    }

    @Override
    public boolean isFloatingPoint() {
        return false;
    }

    public Set<Dimension> getDependentDimensions() {
        return dimensions;
    }

    @Override
    public CardinalityAggregation withName(String name) {
        return new CardinalityAggregation(name, dimensions, byRow);
    }
    /**
     * Field name copy constructor is unsupported because cardinality metrics are built using dimensions backed by
     * dimension druid names.
     *
     * @param fieldName  Field name for the new aggregation
     *
     * @return Always throws an UnsupportedOperationException
     */
    @Override
    public CardinalityAggregation withFieldName(String fieldName) {
        throw new UnsupportedOperationException("Cardinality aggregators can only be rewritten with dimensions.");
    }

    /**
     * Build a copy with a modified base set of dimensions.
     *
     * @param dimensions  The dimensions for the new Cardinality Aggregation
     *
     * @return a copy of this aggregation with different underlying dimensions
     */
    public CardinalityAggregation withDimensions(Set<Dimension> dimensions)  {
        return new CardinalityAggregation(getName(), dimensions, byRow);
    }

    /**
     * Build a copy with a given byRow values.
     *
     * @param byRow  The by row value to change to
     *
     * @return a copy of this aggregation with different byRow value
     */
    public CardinalityAggregation withByRow(boolean byRow)  {
        return new CardinalityAggregation(getName(), dimensions, byRow);
    }

    @Override
    public Pair<Optional<Aggregation>, Optional<Aggregation>> nest() {
        return new ImmutablePair<>(Optional.of(this), Optional.empty());
    }

    @Override
    public boolean equals(final Object o) {
        if (o instanceof CardinalityAggregation) {
            CardinalityAggregation that = (CardinalityAggregation) o;
            return getName().equals(that.getName()) && dimensions.equals(that.dimensions) && byRow == that.byRow;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), dimensions, byRow);
    }
}
