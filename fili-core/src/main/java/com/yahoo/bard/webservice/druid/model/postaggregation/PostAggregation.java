// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.FIELD_ACCESS;

import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.util.EnumUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Base class for model representing post aggregations.
 */
public abstract class PostAggregation implements MetricField {

    private static final Logger LOG = LoggerFactory.getLogger(PostAggregation.class);

    private final PostAggregationType type;
    private final String name;

    /**
     * Constructor.
     *
     * @param type  Type of PostAggregation
     * @param name  Name of the post aggregation column. Most PostAggregations must have a name. FieldAccessor may be
     * the only one that does not have a name.
     */
    protected PostAggregation(PostAggregationType type, String name) {
        // Check for `null` name. Only field access doesn't require a name
        if (name == null && type != FIELD_ACCESS) {
            String message = "Post Aggregation name cannot be null";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }

        this.type = type;
        this.name = name;
    }

    public PostAggregationType getType() {
        return type;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    @JsonIgnore
    public boolean isSketch() {
        return false;
    }

    @Override
    @JsonIgnore
    public boolean isFloatingPoint() {
        return true;
    }

    /**
     * Get a new instance of this PostAggregation with the given name.
     *
     * @param name  Name of the new PostAggregation.
     *
     * @return a new instance of this with the given name
     */
    @Override
    public abstract PostAggregation withName(String name);

    @Override
    public String toString() {
        return "PostAggregation{type=" + getType() + ", name=" + getName() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PostAggregation)) {
            return false;
        }

        PostAggregation that = (PostAggregation) o;

        return
                Objects.equals(name, that.name) &&
                type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name);
    }

    /**
     * Default types of post aggregations.
     */
    public enum DefaultPostAggregationType implements PostAggregationType {
        ARITHMETIC,
        FIELD_ACCESS,
        CONSTANT,
        SKETCH_ESTIMATE,
        SKETCH_SET_OPER,
        THETA_SKETCH_ESTIMATE,
        THETA_SKETCH_SET_OP;

        private final String jsonName;

        /**
         * Constructor.
         */
        DefaultPostAggregationType() {
            this.jsonName = EnumUtils.enumJsonName(this);
        }

        @Override
        @JsonValue
        public String toJson() {
            return jsonName;
        }
    }
}
