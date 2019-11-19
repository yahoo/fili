// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Class for specifying the ContainsSearchQuerySpec.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "type", "case_sensitive", "value" })
public class ContainsSearchQuerySpec extends SearchQuerySpec {

    private final Boolean caseSensitive;
    private final String value;

    /**
     * Constructs a new {@code ContainsSearchQuerySpec} with the specified flag on case-sensitive search and value to
     * search for.
     * <p>
     * Both flag and value can be {@code null}.
     *
     * @param caseSensitive a flag indicating whether or not the search should be case sensitive
     * @param value value to search for
     */
    public ContainsSearchQuerySpec(final Boolean caseSensitive, final String value) {
        super(DefaultSearchQueryType.CONTAINS);
        this.caseSensitive = caseSensitive;
        this.value = value;
    }

    /**
     * Returns true if the search is case sensitive.
     *
     * @return the flag indicating whether or not the search should be case sensitive
     */
    @JsonProperty(value = "case_sensitive")
    public Boolean isCaseSensitive() {
        return caseSensitive;
    }

    /**
     * Returns the value to search for in this search query spec.
     *
     * @return the searched value
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns the string representation of this search query spec.
     * <p>
     * <b>The string is NOT the JSON representation used for Druid query.</b> The format of the string is
     * "ContainsSearchQuerySpec{type=XXX, case_sensitive=YYY, value='ZZZ'}", where XXX is given by {@link #getType()},
     * YYY by {@link #isCaseSensitive()}, and ZZZ by {@link #getValue()} which is also surrounded by a pair of single
     * quotes. Note that each value is separated by a comma followed by a single space.
     *
     * @return the string representation of this search query spec
     */
    @Override
    public String toString() {
        return String.format(
                "ContainsSearchQuerySpec{type=%s, case_sensitive=%s, value='%s'}",
                getType(),
                isCaseSensitive(),
                getValue()
        );
    }
}
