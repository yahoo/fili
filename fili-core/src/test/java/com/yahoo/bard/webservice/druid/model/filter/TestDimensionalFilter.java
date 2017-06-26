package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;

/**
 * A dummy test class that simply constructs a dimensional filter.
 */
public class TestDimensionalFilter extends DimensionalFilter {

    private FilterType type;

    public TestDimensionalFilter(Dimension dimension, FilterType type) {
        super(dimension, type);
        this.type = type;
    }

    @Override
    public DimensionalFilter withDimension(final Dimension dimension) {
        return new TestDimensionalFilter(dimension, type);
    }
}
