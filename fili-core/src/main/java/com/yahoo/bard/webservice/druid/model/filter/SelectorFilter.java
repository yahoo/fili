// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;

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

    /**
     * Constructor.
     *
     * @param dimension  Dimension to apply the extraction to
     * @param value  Value of the filter
     * @param extractionFn  Extraction function to be applied on dimension
     */
    public SelectorFilter(Dimension dimension, String value, ExtractionFunction extractionFn) {
        super(dimension, DefaultFilterType.SELECTOR, extractionFn);
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public SelectorFilter withDimension(Dimension dimension) {
        return new SelectorFilter(dimension,  value, getExtractionFunction());
    }

    /**
     * Get a new instance of this filter with the given value.
     *
     * @param value  Value of the new filter.
     *
     * @return a new instance of this filter with the given value
     */
    public SelectorFilter withValue(String value) {
        return new SelectorFilter(getDimension(), value, getExtractionFunction());
    }

    /**
     * Get a new instance of this filter with the given value.
     *
     * @param extractionFn  Extraction function to be applied on dimension
     *
     * @return a new instance of this filter with the given value
     */
    public SelectorFilter withExtractionFn(ExtractionFunction extractionFn) {
        return new SelectorFilter(getDimension(), value, extractionFn);
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

    @Override
    public String toString() {
        return "Filter{ type=" + getType() + ", dimension=" + getDimension() + ", value=" + getValue() + "}";
    }
}
