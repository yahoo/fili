// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Filtered DimensionSpec using Regex pattern on Druid multi-dimensional value dimensions.
 */
public class RegexFilteredDimensionSpec extends DimensionSpec {
    private final DimensionSpec delegate;
    private final Pattern pattern;

    /**
     * Constructor.
     *
     * @param delegate  dimension spec provided by user.
     * @param pattern  regex pattern to be used for filtering.
     */
    public RegexFilteredDimensionSpec(DimensionSpec delegate, Pattern pattern) {
        super(DefaultDimensionSpecType.REGEX_FILTERED);
        this.delegate = delegate;
        this.pattern = pattern;
    }

    public DimensionSpec getDelegate() {
        return delegate;
    }

    public Pattern getPattern() {
        return pattern;
    }

    @JsonIgnore
    @Override
    public Optional<Dimension> getConfigDimension() {
        return getDelegate().getConfigDimension();
    }

    // CHECKSTYLE:OFF
    public RegexFilteredDimensionSpec withDelegate(DimensionSpec delegate) {
        return new RegexFilteredDimensionSpec(delegate, pattern);
    }

    public RegexFilteredDimensionSpec withPattern(Pattern pattern) {
        return new RegexFilteredDimensionSpec(delegate, pattern);
    }
    // CHECKSTYLE:ON

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), delegate, (pattern == null) ? null : pattern.pattern());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        RegexFilteredDimensionSpec other = (RegexFilteredDimensionSpec) obj;
        // CHECKSTYLE:OFF
        return super.equals(obj) &&
                Objects.equals(delegate, other.delegate) &&
                (pattern == null ? other.pattern == null : other.pattern != null && Objects.equals(pattern.pattern(), other.pattern.pattern()));
        // CHECKSTYLE:ON
    }
}
