// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.descriptor;

import com.yahoo.bard.webservice.data.dimension.DimensionField;

import java.util.Objects;

/**
 * Everything you need to know about a Dimension Field.
 *
 * Combined with DimensionDescriptor, can be used to construct an actual
 * DimensionConfig object.
 *
 * Happens to implement DimensionField for convenience.
 */
public class DimensionFieldDescriptor implements DimensionField {

    protected final String name;
    protected final String description;

    // Include by default in query results (overrideable in dimension)
    protected final boolean queryIncludedByDefault;

    // Include by default in dimension configurations (overrideable in dimension)
    protected final boolean dimensionIncludedByDefault;

    /**
     * Construct a new dimension field configuration object.
     *
     * @param name  the field name
     * @param description  The field description
     * @param queryIncludedByDefault  Whether this field is included by default in query results
     * @param dimensionIncludedByDefault  Whether this field is included by default on all dimensions
     */
    public DimensionFieldDescriptor(
            String name,
            String description,
            boolean queryIncludedByDefault,
            boolean dimensionIncludedByDefault) {
        this.name = name;
        this.description = description;
        this.queryIncludedByDefault = queryIncludedByDefault;
        this.dimensionIncludedByDefault = dimensionIncludedByDefault;
    }

    /**
     * Get the field name.
     *
     * @return  The name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the field description.
     *
     * @return  The description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Return true if the metric should be included in query results by default.
     *
     * @return  True if included by default
     */
    public boolean isQueryIncludedByDefault() {
        return queryIncludedByDefault;
    }

    /**
     * Return true if the metric should be included in all dimensions by default.
     *
     * @return  True if included by default
     */
    public boolean isDimensionIncludedByDefault() {
        return dimensionIncludedByDefault;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        final DimensionFieldDescriptor that = (DimensionFieldDescriptor) o;
        return queryIncludedByDefault == that.queryIncludedByDefault &&
                dimensionIncludedByDefault == that.dimensionIncludedByDefault &&
                Objects.equals(name, that.name) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, queryIncludedByDefault, dimensionIncludedByDefault);
    }
}
