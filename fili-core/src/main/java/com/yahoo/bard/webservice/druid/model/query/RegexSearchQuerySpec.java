// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Class for specifying the RegexSearchQuerySpec for DruidSearchQuery.
 */
public class RegexSearchQuerySpec extends SearchQuerySpec {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String pattern;

    /**
     * Constructor.
     *
     * @param pattern  Pattern for the regex to match
     */
    public RegexSearchQuerySpec(String pattern) {
        super(DefaultSearchQueryType.REGEX);
        this.pattern = pattern;
    }

    public String getPattern() {
        return pattern;
    }
}
