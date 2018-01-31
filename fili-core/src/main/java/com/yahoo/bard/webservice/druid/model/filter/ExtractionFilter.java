// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;

import java.util.Objects;

/**
 * Filter for matching a dimension using some specific Extraction function.
 *
 * @deprecated  Use {@link SelectorFilter} dimensional filters with extractionFn specified instead
 */
@Deprecated
public class ExtractionFilter extends DimensionalFilter {

    private final String value;

    /**
     * Constructor.
     *
     * @param dimension  Dimension to apply the extraction to
     * @param value  Value of the filter
     * @param extractionFn  Function to do the extraction
     */
    public ExtractionFilter(Dimension dimension, String value, ExtractionFunction extractionFn) {
        super(dimension, DefaultFilterType.EXTRACTION, extractionFn);
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public ExtractionFilter withDimension(Dimension dimension) {
        return new ExtractionFilter(dimension, value, getExtractionFunction());
    }

    /**
     * Get a new instance of this filter with the given value.
     *
     * @param value  Value of the new filter.
     *
     * @return a new instance of this filter with the given value
     */
    public ExtractionFilter withValue(String value) {
        return new ExtractionFilter(getDimension(), value, getExtractionFunction());
    }

    /**
     * Get a new instance of this filter with the given ExtractionFunction.
     *
     * @param extractionFunction  ExtractionFunction of the new filter.
     *
     * @return a new instance of this filter with the given extraction function
     */
    public ExtractionFilter withExtractionFunction(ExtractionFunction extractionFunction) {
        return new ExtractionFilter(getDimension(), value, extractionFunction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        ExtractionFilter other = (ExtractionFilter) obj;

        return
                super.equals(obj) &&
                Objects.equals(value, other.value);
    }
}
