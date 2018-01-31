// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.impl.ExtractionFunctionDimension;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;
import com.yahoo.bard.webservice.druid.serializers.DimensionToNameSerializer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Objects;
import java.util.Optional;

/**
 * Filter for matching a dimension.
 *
 * @param <T> a DimensionalFilter
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class DimensionalFilter<T extends DimensionalFilter<? super T>> extends Filter {

    private final Dimension dimension;
    private final ExtractionFunction extractionFunction;

    /**
     * Constructor, default extraction function to the one on dimension if it has one.
     *
     * @param dimension  Dimension to filter
     * @param type Type of the filter
     */
    protected DimensionalFilter(Dimension dimension, FilterType type) {
        super(type);

        this.dimension = dimension;

        if (dimension instanceof ExtractionFunctionDimension) {
            Optional<ExtractionFunction> optionalExtractionFunction = ((ExtractionFunctionDimension) dimension)
                    .getExtractionFunction();
            this.extractionFunction = optionalExtractionFunction.isPresent() ? optionalExtractionFunction.get() : null;
        } else {
            this.extractionFunction = null;
        }
    }

    /**
     * Constructor, with explicit extraction function provided.
     *
     * @param dimension  Dimension to filter
     * @param type Type of the filter
     * @param extractionFunction  Extraction function to be applied on dimension
     */
    protected DimensionalFilter(Dimension dimension, FilterType type, ExtractionFunction extractionFunction) {
        super(type);
        this.dimension = dimension;
        this.extractionFunction = extractionFunction;
    }

    @JsonSerialize(using = DimensionToNameSerializer.class)
    public Dimension getDimension() {
        return dimension;
    }

    @JsonProperty(value = "extractionFn")
    public ExtractionFunction getExtractionFunction() {
        return extractionFunction;
    }

    /**
     * Get a new instance of this filter with the given Dimension.
     *
     * @param dimension  Dimension of the new filter
     *
     * @return a new instance of this filter with the given dimension
     */
    public abstract T withDimension(Dimension dimension);

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dimension, extractionFunction);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        DimensionalFilter other = (DimensionalFilter) obj;

        return
                super.equals(obj) &&
                        Objects.equals(dimension, other.dimension) &&
                        Objects.equals(extractionFunction, other.extractionFunction);
    }
}
