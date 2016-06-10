// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;

/**
 * Filter for exactly matching a column and value in druid
 */
public class SelectorFilter extends Filter {

    private final Dimension dimension;

    private final String value;

    public SelectorFilter(Dimension dimension, String value) {
        super(DefaultFilterType.SELECTOR);
        this.dimension = dimension;
        this.value = value;
    }

    public Dimension getDimension() {
        return dimension;
    }

    public String getValue() {
        return value;
    }

    public SelectorFilter withDimension(Dimension dimension) {
        return new SelectorFilter(dimension,  value);
    }

    public SelectorFilter withValue(String value) {
        return new SelectorFilter(dimension, value);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((dimension == null) ? 0 : dimension.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        SelectorFilter other = (SelectorFilter) obj;
        if (dimension == null) {
            if (other.dimension != null) { return false; }
        } else if (!dimension.equals(other.dimension)) { return false; }
        if (value == null) {
            if (other.value != null) { return false; }
        } else if (!value.equals(other.value)) { return false; }
        return true;
    }
}
