// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import java.util.regex.Pattern;

/**
 * Filter for matching a regular expression against a child filter expression.
 */
public class RegularExpressionFilter extends Filter {

    private final Dimension dimension;

    private final Pattern pattern;

    public RegularExpressionFilter(Dimension dimension, Pattern pattern) {
        super(DefaultFilterType.REGEX);
        this.dimension = dimension;
        this.pattern = pattern;
    }

    public Dimension getDimension() {
        return dimension;
    }

    public Pattern getPattern() {
        return pattern;
    }

    public RegularExpressionFilter withDimension(Dimension dimension) {
        return new RegularExpressionFilter(dimension,  pattern);
    }

    public RegularExpressionFilter withPattern(Pattern pattern) {
        return new RegularExpressionFilter(dimension, pattern);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((dimension == null) ? 0 : dimension.hashCode());
        result = prime * result + ((pattern == null) ? 0 : pattern.pattern().hashCode());
        return result;
    }

    @Override
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        RegularExpressionFilter other = (RegularExpressionFilter) obj;
        if (dimension == null ? other.dimension != null : !dimension.equals(other.dimension)) { return false; }
        // CHECKSTYLE:OFF
        if (pattern == null ? other.pattern != null : other.pattern == null || !pattern.pattern().equals(other.pattern.pattern())) { return false; }
        // CHECKSTYLE:ON
        return true;
    }
}
