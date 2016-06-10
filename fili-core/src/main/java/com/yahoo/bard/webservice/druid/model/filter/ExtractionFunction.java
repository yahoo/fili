// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.util.EnumUtils;

import com.fasterxml.jackson.annotation.JsonValue;

/*
 Extraction functions define the transformation applied to each dimension value
 */
public abstract class ExtractionFunction {
    private final ExtractionFunctionType type;

    protected ExtractionFunction(ExtractionFunctionType type) {
        this.type = type;
    }

    public ExtractionFunctionType getType() {
        return type;
    }

    public enum ExtractionFunctionType {
        REGEX,
        PARTIAL,
        SEARCH_QUERY,
        TIME_FORMAT,
        TIME,
        JAVASCRIPT,
        LOOKUP;

        final String jsonName;

        ExtractionFunctionType() {
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
        ExtractionFunction other = (ExtractionFunction) obj;
        if (type != other.type) { return false; }
        return true;
    }
}
