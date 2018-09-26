// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Cascade ExtractionFunction that chains an array of ExtractionFunctions to be executed in array index order.
 */
public class CascadeExtractionFunction extends ExtractionFunction {
    private final List<ExtractionFunction> extractionFunctions;

    /**
     * Constructor.
     *
     * @param extractionFunctions  A list of extraction functions to be executed in index order.
     */
    public CascadeExtractionFunction(List<ExtractionFunction> extractionFunctions) {
        super(DefaultExtractionFunctionType.CASCADE);
        this.extractionFunctions = Collections.unmodifiableList(extractionFunctions);
    }

    @JsonProperty(value = "extractionFns")
    public List<ExtractionFunction> getExtractionFunctions() {
        return extractionFunctions;
    }

    // CHECKSTYLE:OFF
    public CascadeExtractionFunction withExtractionFunctions(List<ExtractionFunction> extractionFunctions) {
        return new CascadeExtractionFunction(extractionFunctions);
    }
    // CHECKSTYLE:ON

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), extractionFunctions);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        CascadeExtractionFunction other = (CascadeExtractionFunction) obj;
        return super.equals(obj) && Objects.equals(extractionFunctions, other.extractionFunctions);
    }
}
