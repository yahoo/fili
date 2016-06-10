// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.FIELD_ACCESS;

import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.util.EnumUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for model representing post aggregations.
 */
public abstract class PostAggregation implements MetricField {

    private static final Logger LOG = LoggerFactory.getLogger(PostAggregation.class);

    protected final PostAggregationType type;

    protected final String name;

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
    public String toString() {
        return "PostAggregation{type=" + type + ", name=" + name + "}";
    }

    @Override
    public String getName() {
        return name;
    }

    public abstract PostAggregation withName(String name);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PostAggregation)) {
            return false;
        }

        PostAggregation that = (PostAggregation) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }

        if (type != that.type) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
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

        DefaultPostAggregationType() {
            this.jsonName = EnumUtils.enumJsonName(this);
        }

        @Override
        @JsonValue
        public String toJson() {
            return jsonName;
        }
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
}
