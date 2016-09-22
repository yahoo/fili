// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import java.util.Objects;

/**
 * Filter for exactly matching a column and value in druid.
 */
public class SelectorFilter extends DimensionalFilter {

    private final String value;

    /**
     * Constructor.
     *
     * @param dimension  Dimension to apply the extraction to
     * @param value  Value of the filter
     */
    public SelectorFilter(Dimension dimension, String value) {
        super(dimension, DefaultFilterType.SELECTOR);
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public SelectorFilter withDimension(Dimension dimension) {
        return new SelectorFilter(dimension,  value);
    }

    /**
     * Get a new instance of this filter with the given value.
     *
     * @param value  Value of the new filter.
     *
     * @return a new instance of this filter with the given value
     */
    public SelectorFilter withValue(String value) {
        return new SelectorFilter(getDimension(), value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        SelectorFilter other = (SelectorFilter) obj;
        return
                super.equals(obj) &&
                Objects.equals(value, other.value);
    }
}
