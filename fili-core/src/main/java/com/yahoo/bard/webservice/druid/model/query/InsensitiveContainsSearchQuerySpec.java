// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Class for specifying the InsensitiveContainsSearchQuerySpec for DruidSearchQuery.
 */
public class InsensitiveContainsSearchQuerySpec extends SearchQuerySpec {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public final String value;

    /**
     * Constructor.
     *
     * @param value  Value to search for case-insensitively
     */
    public InsensitiveContainsSearchQuerySpec(String value) {
        super(DefaultSearchQueryType.INSENSITIVE_CONTAINS);
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
