// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction;

import com.yahoo.bard.webservice.util.EnumUtils;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

/**
 * Extraction functions define the transformation applied to each dimension value.
 */
public abstract class ExtractionFunction {
    private final ExtractionFunctionType type;

    /**
     * Constructor.
     *
     * @param type  Type of this ExtractionFunction
     */
    protected ExtractionFunction(ExtractionFunctionType type) {
        this.type = type;
    }

    public ExtractionFunctionType getType() {
        return type;
    }

    /**
     * Enumeration of possible extraction function types.
     */
    public enum DefaultExtractionFunctionType implements ExtractionFunctionType {

        /**
         * Regular expression extraction function.
         * <p>
         * See {@link RegularExpressionExtractionFunction}.
         */
        REGEX,

        /**
         * Partial extraction function.
         * <p>
         * See {@link PartialExtractionFunction}.
         */
        PARTIAL,
        SEARCH_QUERY,
        TIME_FORMAT,
        TIME,
        JAVASCRIPT,
        CASCADE,
        LOOKUP,


        REGISTERED_LOOKUP
        ;

        final String jsonName;

        /**
         * Constructor.
         */
        DefaultExtractionFunctionType() {
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
        ExtractionFunction other = (ExtractionFunction) obj;
        return Objects.equals(type, other.type);
    }
}
