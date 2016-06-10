// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.CONSTANT;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import java.util.Collections;
import java.util.Set;

/**
 * Model representing post aggregations with a constant value.
 */
public class ConstantPostAggregation extends PostAggregation {

    private final double value;

    public ConstantPostAggregation(String name, double value) {
        super(CONSTANT, name);
        this.value = value;
    }

    public double getValue() {
        return value;
    }

    @Override
    public ConstantPostAggregation withName(String name) {
        return new ConstantPostAggregation(name, value);
    }

    public ConstantPostAggregation withValue(double value) {
        return new ConstantPostAggregation(getName(), value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof ConstantPostAggregation)) { return false; }
        if (!super.equals(o)) { return false; }

        ConstantPostAggregation that = (ConstantPostAggregation) o;

        if (Double.compare(that.value, value) != 0) { return false; }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        long temp;
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public Set<Dimension> getDependentDimensions() {
        return Collections.emptySet();
    }
}
