// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.serializers.DimensionToNameSerializer;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import javax.validation.constraints.NotNull;

/**
 * Filter for matching a dimension.
 *
 * @param <T> a DimensionalFilter
 */
public abstract class DimensionalFilter<T extends DimensionalFilter<? super T>> extends Filter {

    private static final Logger LOG = LoggerFactory.getLogger(DimensionalFilter.class);
    private final Dimension dimension;

    /**
     * Constructor.
     *
     * @param dimension  Dimension to filter
     * @param type Type of the filter
     */
    protected DimensionalFilter(@NotNull Dimension dimension, FilterType type) {
        super(type);

        if (Objects.isNull(dimension)) {
            String message = ErrorMessageFormat.FILTER_DIMENSION_UNDEFINED.format(dimension);
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }

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
