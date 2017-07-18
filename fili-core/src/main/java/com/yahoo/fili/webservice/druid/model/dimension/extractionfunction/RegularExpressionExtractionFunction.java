// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.druid.model.dimension.extractionfunction;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Regular Expression ExtractionFunction that returns the first matching group for the given regular expression.
 * If there is no match, it returns the dimension value as is
 */
public class RegularExpressionExtractionFunction extends ExtractionFunction {
    private final Pattern pattern;

    /**
     * Constructor.
     *
     * @param pattern  Regex Pattern of the extraction function
     */
    public RegularExpressionExtractionFunction(Pattern pattern) {
        super(DefaultExtractionFunctionType.REGEX);
        this.pattern = pattern;
    }

    @JsonProperty(value = "expr")
    public Pattern getPattern() {
        return pattern;
    }

    /**
     * Get a new instance of this filter with the given pattern.
     *
     * @param pattern  Pattern of the new filter.
     *
     * @return a new instance of this filter with the given pattern
     */
    public RegularExpressionExtractionFunction withPattern(Pattern pattern) {
        return new RegularExpressionExtractionFunction(pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), (pattern == null) ? null : pattern.pattern());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        RegularExpressionExtractionFunction other = (RegularExpressionExtractionFunction) obj;
        // CHECKSTYLE:OFF
        return super.equals(obj) &&
                (pattern == null ? other.pattern == null : other.pattern != null && Objects.equals(pattern.pattern(), other.pattern.pattern()));
        // CHECKSTYLE:ON
    }
}
