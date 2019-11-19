// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model;

import com.yahoo.bard.webservice.util.EnumUtils;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Ordering specified for the range filtering.
 */
public enum Ordering {
    LEXICOGRAPHIC,
    ALPHANUMERIC,
    NUMERIC,
    STRLEN;

    final String orderingType;

    /**
     * Constructor.
     */
    Ordering() {
        this.orderingType = EnumUtils.enumJsonName(this);
    }

    /**
     * Get the JSON representation of this class.
     *
     * @return the JSON representation.
     */
    @JsonValue
    public String toJson() {
        return orderingType;
    }
}
