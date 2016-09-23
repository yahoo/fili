// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Filter for matching a dimension using some specific Extraction function.
 */
public class ExtractionFilter extends DimensionalFilter {

    private final String value;

    private final ExtractionFunction extractionFunction;

    /**
     * Constructor.
     *
     * @param dimension  Dimension to apply the extraction to
     * @param value  Value of the filter
     * @param extractionFn  Function to do the extraction
     */
    public ExtractionFilter(Dimension dimension, String value, ExtractionFunction extractionFn) {
        super(dimension, DefaultFilterType.EXTRACTION);
        this.value = value;
        this.extractionFunction = extractionFn;
    }

    public String getValue() {
        return value;
    }

    @JsonProperty(value = "extractionFn")
    public ExtractionFunction getExtractionFunction() {
        return extractionFunction;
    }

    @Override
    public ExtractionFilter withDimension(Dimension dimension) {
        return new ExtractionFilter(dimension, value, extractionFunction);
    }

    /**
     * Get a new instance of this filter with the given value.
     *
     * @param value  Value of the new filter.
     *
     * @return a new instance of this filter with the given value
     */
    public ExtractionFilter withValue(String value) {
        return new ExtractionFilter(getDimension(), value, extractionFunction);
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
        return Objects.hash(super.hashCode(), value, extractionFunction);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        ExtractionFilter other = (ExtractionFilter) obj;

        return
                super.equals(obj) &&
                Objects.equals(value, other.value) &&
                Objects.equals(extractionFunction, other.extractionFunction);
    }
}
