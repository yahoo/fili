// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;
import java.util.Optional;

/**
 * The Default type DimensionSpec.
 */
public class DefaultDimensionSpec extends DimensionSpec {
    private final String dimension;
    private final String outputName;
    protected final Dimension configDimension;

    /**
     * Constructor.
     *
     * @param dimension  dimension name to be selected.
     * @param outputName  replace output dimension name with this value.
     */
    public DefaultDimensionSpec(String dimension, String outputName) {
        this(dimension, outputName, null);
    }

    /**
     * Constructor.
     *
     * @param dimension  dimension name to be selected.
     * @param outputName  replace output dimension name with this value.
     * @param configDimension  the Dimension object associated with the dimension name provided in "dimension"
     * param
     */
    public DefaultDimensionSpec(String dimension, String outputName, Dimension configDimension) {
        super(DefaultDimensionSpecType.DEFAULT);
        this.dimension = dimension;
        this.outputName = outputName;
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

    // CHECKSTYLE:OFF
    public DefaultDimensionSpec withDimension(String dimension) {
        return new DefaultDimensionSpec(dimension, outputName, configDimension);
    }

    public DefaultDimensionSpec withOutputName(String outputName) {
        return new DefaultDimensionSpec(dimension, outputName, configDimension);
    }

    public DefaultDimensionSpec withConfigDimension(Dimension configDimension) {
        return new DefaultDimensionSpec(dimension, outputName, configDimension);
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
