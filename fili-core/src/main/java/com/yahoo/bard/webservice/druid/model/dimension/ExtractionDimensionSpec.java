// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

/**
 * DimensionSpec using ExtractionFunctions.
 */
public class ExtractionDimensionSpec extends DimensionSpec {
    private final String dimension;
    private final String outputName;
    private final ExtractionFunction extractionFunction;
    protected final Dimension configDimension;

    /**
     * Constructor.
     *
     * @param dimension  name of the dimension to be selected.
     * @param outputName  replace output dimension name with this value.
     * @param extractionFunction  extraction function to be applied to this dimension.
     */
    public ExtractionDimensionSpec(String dimension, String outputName, ExtractionFunction extractionFunction) {
        this(dimension, outputName, extractionFunction, null);
    }

    /**
     * Constructor.
     *
     * @param dimension  name of the dimension to be selected.
     * @param outputName  replace output dimension name with this value.
     * @param extractionFunction  extraction function to be applied to this dimension.
     * @param configDimension  the Dimension object associated with the dimension name provided in "dimension"
     * param
     */
    public ExtractionDimensionSpec(
            String dimension,
            String outputName,
            ExtractionFunction extractionFunction,
            Dimension configDimension
    ) {
        super(DefaultDimensionSpecType.EXTRACTION);
        this.dimension = dimension;
        this.outputName = outputName;
        this.extractionFunction = extractionFunction;
        this.configDimension = configDimension;
    }

    public String getDimension() {
        return dimension;
    }

    public String getOutputName() {
        return outputName;
    }

    @JsonIgnore
    @Override
    public Optional<Dimension> getConfigDimension() {
        return Optional.ofNullable(configDimension);
    }

    @JsonProperty(value = "extractionFn")
    public ExtractionFunction getExtractionFunction() {
        return extractionFunction;
    }

    // CHECKSTYLE:OFF
    public ExtractionDimensionSpec withDimension(String dimension) {
        return new ExtractionDimensionSpec(dimension, outputName, extractionFunction, configDimension);
    }

    public ExtractionDimensionSpec withOutputName(String outputName) {
        return new ExtractionDimensionSpec(dimension, outputName, extractionFunction, configDimension);
    }

    public ExtractionDimensionSpec withExtractionFunction(ExtractionFunction extractionFunction) {
        return new ExtractionDimensionSpec(dimension, outputName, extractionFunction, configDimension);
    }

    public ExtractionDimensionSpec withConfigDimension(Dimension configDimension) {
        return new ExtractionDimensionSpec(dimension, outputName, extractionFunction, configDimension);
    }
    // CHECKSTYLE:ON

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dimension, outputName, extractionFunction);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        ExtractionDimensionSpec other = (ExtractionDimensionSpec) obj;
        return super.equals(obj) &&
                Objects.equals(dimension, other.dimension) &&
                Objects.equals(outputName, other.outputName) &&
                Objects.equals(extractionFunction, other.extractionFunction);
    }
}
