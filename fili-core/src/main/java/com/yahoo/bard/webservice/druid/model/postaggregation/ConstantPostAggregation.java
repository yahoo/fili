// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.CONSTANT;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Model representing post aggregations with a constant value.
 */
public class ConstantPostAggregation extends PostAggregation {

    private final double value;

    /**
     * Constructor.
     *
     * @param name  Name of the post-aggregation
     * @param value  Constant value of the post-aggregation
     */
    public ConstantPostAggregation(String name, double value) {
        super(CONSTANT, name);
        this.value = value;
    }

    public double getValue() {
        return value;
    }

    @Override
    public Set<Dimension> getDependentDimensions() {
        return Collections.emptySet();
    }

    @Override
    public ConstantPostAggregation withName(String name) {
        return new ConstantPostAggregation(name, value);
    }

    /**
     * Get a new instance of this with the given value.
     *
     * @param value  Value of the new instance.
     *
     * @return a new instance of this with the given value
     */
    public ConstantPostAggregation withValue(double value) {
        return new ConstantPostAggregation(getName(), value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof ConstantPostAggregation)) { return false; }
        ConstantPostAggregation that = (ConstantPostAggregation) o;
        return super.equals(o) &&
                Double.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }
}
