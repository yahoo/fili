// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Regular Expression ExtractionFunction that returns the first matching group for the given regular expression.
 * <p>
 * If there is no match, it returns the dimension value as is. The actual serialized extraction function will be
 * <pre>
 * {@code
 *     {
 *         "type": "regex",
 *         "expr": <regular_expression>,
 *         "index": <integer for group to extract, default 1>,
 *         "replaceMissingValue": <default false>,
 *         "replaceMissingValueWith": <default ignored>
 *     }
 * }
 * </pre>
 * <b>Note that the {@code index}, {@code replaceMissingValue}, and {@code replaceMissingValueWith} are using default
 * values and are not included in the serialization</b>.
 */
public class RegularExpressionExtractionFunction extends ExtractionFunction {

    private final Pattern pattern;
    private final Integer index;
    private final String replaceValue;

    /**
     * Constructor.
     *
     * @param pattern  Regex Pattern of the extraction function
     */
    public RegularExpressionExtractionFunction(Pattern pattern) {
        this(pattern, null, null);
    }

    /**
     * Constructor.
     *
     * @param pattern  Regex Pattern of the extraction function
     * @param index  The matching capture expression from the pattern
     * @param replaceValue  The value to replace non-matches with
     */
    public RegularExpressionExtractionFunction(Pattern pattern, Integer index, String replaceValue) {
        super(DefaultExtractionFunctionType.REGEX);
        this.pattern = pattern;
        this.index = index;
        this.replaceValue = replaceValue;
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
     * Which capture pattern from the reg ex to match on.
     *
     * @return the regex pattern of this extraction function
     */
    @JsonProperty(value = "index")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getIndex() {
        return index;
    }

    /**
     * If there is a replace value specified, set replaceMissingValue to true.
     *
     * @return the regex pattern of this extraction function
     */
    @JsonProperty(value = "replaceMissingValue")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Boolean isReplaceMissingValue() {
        return replaceValue == null ? null : Boolean.TRUE;
    }

    /**
     * The value to replace non-matches with.
     *
     * @return the regex pattern of this extraction function
     */
    @JsonProperty(value = "replaceMissingValueWith")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getRreplaceMissingValueWith() {
        return replaceValue;
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
        return Objects.hash(
                super.hashCode(),
                (pattern == null) ? null : pattern.pattern(),
                index,
                replaceValue
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        RegularExpressionExtractionFunction other = (RegularExpressionExtractionFunction) obj;

        boolean patternsMatch = (pattern == null ?
                                    other.pattern == null
                                : other.pattern != null
                                    && Objects.equals(pattern.pattern(), other.pattern.pattern()));
        // CHECKSTYLE:OFF
        return super.equals(obj)
                && patternsMatch
                && Objects.equals(index, other.index)
                && Objects.equals(replaceValue, other.replaceValue);
        // CHECKSTYLE:ON
    }

    /**
     * Returns the string representation of this extraction function.
     * <p>
     * The format of the string is "RegexExtractionFunction{pattern=XXX}", where XXX is the string representation of
     * the regex pattern of this extraction function.
     *
     * @return the string representation of this extraction function
     */
    @Override
    public String toString() {
        String idx =  (this.index == null) ? "" : String.format(",index=%d", getIndex());
        String replace = (this.replaceValue == null) ? "" : String.format(",replace=%s", getRreplaceMissingValueWith());
        return String.format("RegularExpressionExtractionFunction{pattern=%s%s%s}", getPattern(), idx, replace);
    }
}
