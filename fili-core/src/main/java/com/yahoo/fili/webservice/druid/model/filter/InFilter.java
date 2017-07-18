// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.druid.model.filter;

import com.yahoo.fili.webservice.data.dimension.Dimension;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.validation.constraints.NotNull;

/**
 * Fili model of the Druid In Filter: http://druid.io/docs/0.9.1.1/querying/filters.html.
 * <p>
 * An In Filter is a generalization of the {@link SelectorFilter}. Rather than filtering on a specific value for a
 * specific diimension, the In Filter takes a list of values. A dimension satisfies the In Filter iff its value is
 * contained in the specified list. It is logically equivalent to an {@link OrFilter} wrapped around a collection of
 * {@link SelectorFilter}.
 * <p>
 * Note that Druid's in filter is only supported by Druid versions 0.9.0 and greater.
 */
public class InFilter extends DimensionalFilter<InFilter> {

    private final List<String> values;

    /**
     * Constructor.
     *
     * @param dimension  The dimension to perform an in filter on
     * @param values  The values to filter on
     */
    public InFilter(Dimension dimension, @NotNull List<String> values) {
        super(dimension, DefaultFilterType.IN);
        this.values = values;
    }

    //CHECKSTYLE:OFF
    @Override
    public InFilter withDimension(Dimension dimension) {
        return new InFilter(dimension, getValues());
    }

    public InFilter withValues(List<String> values) {
        return new InFilter(getDimension(), values);
    }
    //CHECKSTYLE:ON

    /**
     * Return the list of values to filter on.
     *
     * @return The list of values to filter on
     */
    public List<String> getValues() {
        return Collections.unmodifiableList(values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), values);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        InFilter inFilter = (InFilter) o;

        return super.equals(inFilter) && Objects.equals(values, inFilter.values);
    }
}
