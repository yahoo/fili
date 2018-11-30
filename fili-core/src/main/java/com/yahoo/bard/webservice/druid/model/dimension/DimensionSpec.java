// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.util.EnumUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;
import java.util.Optional;

/**
 * Base class model for DimensionSpecs.
 */
public abstract class DimensionSpec {
    private final DimensionSpecType type;
    protected final Dimension configDimension;

    /**
     * Constructor.
     *
     * @param type  enum type of this DimensionSpec.
     */
    protected DimensionSpec(DimensionSpecType type) {
        this(type, null);
    }

    /**
     * Constructor.
     *
     * @param type  enum type of this DimensionSpec.
     * @param configDimension  the Dimension object associated with the dimension name provided in "dimension"
     * param
     */
    protected DimensionSpec(DimensionSpecType type, Dimension configDimension) {
        this.type = type;
        this.configDimension = configDimension;
    }

    public DimensionSpecType getType() {
        return type;
    }

    /**
     * Enumaration of possible DimensionSpec types.
     */
    public enum DefaultDimensionSpecType implements DimensionSpecType {
        DEFAULT, EXTRACTION, LIST_FILTERED, REGEX_FILTERED;

        final String jsonName;

        /**
         * Constructor.
         */
        DefaultDimensionSpecType() {
            this.jsonName = EnumUtils.enumJsonName(this);
        }

        /**
         * Get the JSON representation of this class.
         *
         * @return the JSON representation
         */
        @JsonValue
        public String toJson() {
            return jsonName;
        }
    }

    /**
     * Gets the set of dimension names this dimension relies on.
     *
     * @return the dimension names
     */
    @JsonIgnore
    public Optional<Dimension> getConfigDimension() {
        return Optional.ofNullable(configDimension);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        DimensionSpec other = (DimensionSpec) obj;
        return Objects.equals(type, other.type);
    }
}
