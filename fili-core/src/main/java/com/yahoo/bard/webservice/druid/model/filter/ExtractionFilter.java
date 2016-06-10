// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Filter for matching a dimension using some specific Extraction function
 */
public class ExtractionFilter extends Filter {
    private final Dimension dimension;

    private final String value;

    private final ExtractionFunction extractionFunction;

    public ExtractionFilter(Dimension dimension, String value, ExtractionFunction extractionFn) {
        super(DefaultFilterType.EXTRACTION);
        this.dimension = dimension;
        this.value = value;
        this.extractionFunction = extractionFn;
    }

    public Dimension getDimension() {
        return dimension;
    }

    public String getValue() {
        return value;
    }

    @JsonProperty(value = "extractionFn")
    public ExtractionFunction getExtractionFunction() {
        return extractionFunction;
    }

    public ExtractionFilter withDimension(Dimension dimension) {
        return new ExtractionFilter(dimension, value, extractionFunction);
    }

    public ExtractionFilter withValue(String value) {
        return new ExtractionFilter(dimension, value, extractionFunction);
    }

    public ExtractionFilter withExtractionFunction(ExtractionFunction extractionFunction) {
        return new ExtractionFilter(dimension, value, extractionFunction);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((dimension == null) ? 0 : dimension.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        result = prime * result + ((extractionFunction == null) ? 0 : extractionFunction.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        ExtractionFilter other = (ExtractionFilter) obj;
        if (dimension == null ? other.dimension != null : !dimension.equals(other.dimension)) { return false; }
        if (value == null ? other.value != null : !value.equals(other.value)) { return false; }
        if (extractionFunction == null ? other.extractionFunction != null : !extractionFunction.equals(
                other.extractionFunction
        )) {
            return false;
        }
        return true;
    }
}
