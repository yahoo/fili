// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Filter for matching a regular expression against a child filter expression.
 */
public class RegularExpressionFilter extends DimensionalFilter {

    private final Pattern pattern;

    /**
     * Constructor.
     *
     * @param dimension  Dimension to filter
     * @param pattern  Regex pattern to match against the dimension value
     */
    public RegularExpressionFilter(Dimension dimension, Pattern pattern) {
        super(dimension, DefaultFilterType.REGEX);
        this.pattern = pattern;
    }

    public Pattern getPattern() {
        return pattern;
    }

    @Override
    public RegularExpressionFilter withDimension(Dimension dimension) {
        return new RegularExpressionFilter(dimension,  pattern);
    }

    /**
     * Get a new instance of this filter with the given pattern.
     *
     * @param pattern  Pattern of the new filter
     *
     * @return a new instance of this filter with the given pattern
     */
    public RegularExpressionFilter withPattern(Pattern pattern) {
        return new RegularExpressionFilter(getDimension(), pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), (pattern == null) ? null : pattern.pattern());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        RegularExpressionFilter other = (RegularExpressionFilter) obj;
        // CHECKSTYLE:OFF
        return
                super.equals(obj) &&
                (pattern == null ? other.pattern == null : other.pattern != null && Objects.equals(pattern.pattern(), other.pattern.pattern()));
        // CHECKSTYLE:ON
    }
}
