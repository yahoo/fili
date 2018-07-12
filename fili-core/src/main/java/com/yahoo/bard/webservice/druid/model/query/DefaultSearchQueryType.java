// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

/**
 * Enum for specifying the type of SearchQuerySpec.
 */
public enum DefaultSearchQueryType implements SearchQueryType {
    CONTAINS,
    FRAGMENT,
    INSENSITIVE_CONTAINS,
    REGEX;

    private final String lowerCaseName;

    /**
     * Constructor.
     */
    DefaultSearchQueryType() {
        lowerCaseName = name().toLowerCase(Locale.ENGLISH);
    }

    @JsonValue
    @Override
    public String toString() {
       return lowerCaseName;
    }
}
