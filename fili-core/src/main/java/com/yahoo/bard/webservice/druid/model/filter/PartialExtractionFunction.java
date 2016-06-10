// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.regex.Pattern;

/**
 * Regular Expression ExtractionFunction that returns the dimension value if it matches the regular expression.
 * If there is no match, it returns null
 */
public class PartialExtractionFunction extends ExtractionFunction {
    private final Pattern pattern;

    public PartialExtractionFunction(Pattern pattern) {
        super(ExtractionFunctionType.PARTIAL);
        this.pattern = pattern;
    }

    @JsonProperty(value = "expr")
    public Pattern getPattern() {
        return pattern;
    }

    public PartialExtractionFunction withPattern(Pattern pattern) {
        return new PartialExtractionFunction(pattern);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((pattern == null) ? 0 : pattern.pattern().hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        PartialExtractionFunction other = (PartialExtractionFunction) obj;
        // CHECKSTYLE:OFF
        if (pattern == null ? other.pattern != null : other.pattern == null || !pattern.pattern().equals(other.pattern.pattern())) { return false; }
        // CHECKSTYLE:ON
        return true;
    }
}
