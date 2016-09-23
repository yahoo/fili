// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import java.util.Locale;

/**
 * Keywords for the "show" clause in the API.
 */
public enum DimensionFieldSpecifierKeywords {
    ALL,
    NONE;

    private final String name;

    /**
     * Constructor.
     */
    DimensionFieldSpecifierKeywords() {
        name = name().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String toString() {
        return name;
    }
}
