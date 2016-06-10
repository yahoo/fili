// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.util.EnumUtils;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Model representing a filter expression in a druid query.
 */
public abstract class Filter {

    private final FilterType type;

    protected Filter(FilterType type) {
        this.type = type;
    }

    public FilterType getType() {
        return type;
    }

    /**
     * Valid types for druid filters
     */
    public enum DefaultFilterType implements FilterType {
        SELECTOR, REGEX, AND, OR, NOT, EXTRACTION;

        final String jsonName;

        DefaultFilterType() {
            this.jsonName = EnumUtils.enumJsonName(this);
        }

        @JsonValue
        public String toJson() {
            return jsonName;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        Filter other = (Filter) obj;
        if (type != other.type) { return false; }
        return true;
    }
}
