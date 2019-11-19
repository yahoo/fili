// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Partial extraction function returns the dimension value unchanged if the regex matches, otherwise returns null.
 * <pre>
 * {@code
 *     {
 *         "type": "partial",
 *         "expr": <regular_expression>
 *     }
 * }
 * </pre>
 */
public class PartialExtractionFunction extends ExtractionFunction {
    private final Pattern pattern;

    /**
     * Constructor.
     *
     * @param pattern  Regex Pattern of the extraction function
     */
    public PartialExtractionFunction(Pattern pattern) {
        super(DefaultExtractionFunctionType.PARTIAL);
        this.pattern = pattern;
    }

    /**
     * Returns the regex pattern of this extraction function, i.e. the value of {@code expr}.
     *
     * @return the regex pattern of this extraction function
     */
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
    public PartialExtractionFunction withPattern(Pattern pattern) {
        return new PartialExtractionFunction(pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), (pattern == null) ? null : pattern.pattern());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        PartialExtractionFunction other = (PartialExtractionFunction) obj;
        // CHECKSTYLE:OFF
        return super.equals(obj) &&
                (pattern == null ? other.pattern == null : other.pattern != null && Objects.equals(pattern.pattern(), other.pattern.pattern()));
        // CHECKSTYLE:ON
    }

    /**
     * Returns the string representation of this extraction function.
     * <p>
     * The format of the string is "PartialExtractionFunction{pattern=XXX}", where XXX is the string representation of
     * the regex pattern of this extraction function.
     *
     * @return the string representation of this extraction function
     */
    @Override
    public String toString() {
        return String.format("PartialExtractionFunction{pattern=%s}", getPattern());
    }
}
