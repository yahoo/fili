// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;

/**
 * A dummy test class that simply constructs a dimensional filter.
 */
public class TestDimensionalFilter extends DimensionalFilter {

    private FilterType type;

    /**
     * Constructor.
     *
     * @param dimension  Filter dimension passed to super class.
     * @param type  Filter type passed to super class.
     */
    public TestDimensionalFilter(Dimension dimension, FilterType type) {
        super(dimension, type);
        this.type = type;
    }

    @Override
    public DimensionalFilter withDimension(final Dimension dimension) {
        return new TestDimensionalFilter(dimension, type);
    }
}
