// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.util.EnumUtils;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

/**
 * Model representing a filter expression in a druid query.
 */
public abstract class Filter {

    private final FilterType type;

    /**
     * Constructor.
     *
     * @param type  Type of the filter
     */
    protected Filter(FilterType type) {
        this.type = type;
    }

    public FilterType getType() {
        return type;
    }

    /**
     * Valid types for druid filters.
     */
    public enum DefaultFilterType implements FilterType {
        SELECTOR, REGEX, AND, OR, NOT, EXTRACTION, SEARCH, IN, BOUND;

        final String jsonName;

        /**
         * Constructor.
         */
        DefaultFilterType() {
            this.jsonName = EnumUtils.enumJsonName(this);
        }

        /**
         * Get the JSON representation of this class.
         *
         * @return the JSON representation.
         */
        @JsonValue
        public String toJson() {
            return jsonName;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        Filter other = (Filter) obj;
        return type == other.type;
    }
}
