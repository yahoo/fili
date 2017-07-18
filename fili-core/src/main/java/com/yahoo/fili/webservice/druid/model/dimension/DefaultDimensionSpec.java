// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.druid.model.dimension;

import java.util.Objects;

/**
 * The Default type DimensionSpec.
 */
public class DefaultDimensionSpec extends DimensionSpec {
    private final String dimension;
    private final String outputName;

    /**
     * Constructor.
     *
     * @param dimension  dimension name to be selected.
     * @param outputName  replace output dimension name with this value.
     */
    public DefaultDimensionSpec(String dimension, String outputName) {
        super(DefaultDimensionSpecType.DEFAULT);
        this.dimension = dimension;
        this.outputName = outputName;
    }

    public String getDimension() {
        return dimension;
    }

    public String getOutputName() {
        return outputName;
    }

    // CHECKSTYLE:OFF
    public DefaultDimensionSpec withDimension(String dimension) {
        return new DefaultDimensionSpec(dimension, outputName);
    }

    public DefaultDimensionSpec withOutputName(String outputName) {
        return new DefaultDimensionSpec(dimension, outputName);
    }
    // CHECKSTYLE:ON

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dimension, outputName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        DefaultDimensionSpec other = (DefaultDimensionSpec) obj;
        return super.equals(obj) &&
                Objects.equals(dimension, other.dimension) &&
                Objects.equals(outputName, other.outputName);
    }
}
