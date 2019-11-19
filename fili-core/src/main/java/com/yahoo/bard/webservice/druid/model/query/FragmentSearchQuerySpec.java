// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Class for specifying the FragmentSearchQuerySpec for DruidSearchQuery.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "type", "case_sensitive", "values" })
public class FragmentSearchQuerySpec extends SearchQuerySpec {

    private final Boolean caseSensitive;
    private final List<String> values;

    /**
     * Constructs a new {@code FragmentSearchQuerySpec} with the specified fragments to search for.
     * <p>
     * This constructor initializes "case_sensitive" attribute to {@code null} and the attribute will not be included in
     * JSON serialization. The fragments can be {@code null}.
     *
     * @param values fragments to search for
     */
    public FragmentSearchQuerySpec(Collection<String> values) {
        this(null, values);
    }

    /**
     * Constructs a new {@code FragmentSearchQuerySpec} with the specified flag on case-sensitive search and fragments
     * to search for.
     * <p>
     * Both flag and fragments can be {@code null}.
     *
     * @param caseSensitive a flag indicating whether or not the search should be case sensitive
     * @param values fragments to search for
     */
    public FragmentSearchQuerySpec(final Boolean caseSensitive, Collection<String> values) {
        super(DefaultSearchQueryType.FRAGMENT);
        this.caseSensitive = caseSensitive;
        this.values = values == null ? null : Collections.unmodifiableList(new ArrayList<>(values));
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
     * Returns the fragments to search for in the search query spec.
     *
     * @return the searched fragments
     */
    public List<String> getValues() {
        return values;
    }

    /**
     * Returns the string representation of this search query spec.
     * <p>
     * <b>The string is NOT the JSON representation used for Druid query.</b> The format of the string is
     * "FragmentSearchQuerySpec{type=XXX, case_sensitive=YYY, values=ZZZ}", where XXX is given by {@link #getType()},
     * YYY by {@link #isCaseSensitive()}, and ZZZ by {@link #getValues()}. Note that each value is
     * separated by a comma followed by a single space. Values are not quoted.
     *
     * @return the string representation of this search query spec
     */
    @Override
    public String toString() {
        return String.format(
                "FragmentSearchQuerySpec{type=%s, case_sensitive=%s, values=%s}",
                getType(),
                isCaseSensitive(),
                getValues()
        );
    }
}
