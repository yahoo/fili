// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.druid.model.filter;

import com.yahoo.fili.webservice.data.dimension.Dimension;
import com.yahoo.fili.webservice.druid.serializers.DimensionToNameSerializer;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Objects;

/**
 * Filter for matching a dimension.
 *
 * @param <T> a DimensionalFilter
 */
public abstract class DimensionalFilter<T extends DimensionalFilter<? super T>> extends Filter {

    private final Dimension dimension;

    /**
     * Constructor.
     *
     * @param dimension  Dimension to filter
     * @param type Type of the filter
     */
    protected DimensionalFilter(Dimension dimension, FilterType type) {
        super(type);
        this.dimension = dimension;
    }

    @JsonSerialize(using = DimensionToNameSerializer.class)
    public Dimension getDimension() {
        return dimension;
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
        return Objects.hash(super.hashCode(), dimension);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        DimensionalFilter other = (DimensionalFilter) obj;

        return
                super.equals(obj) &&
                Objects.equals(dimension, other.dimension);
    }
}
