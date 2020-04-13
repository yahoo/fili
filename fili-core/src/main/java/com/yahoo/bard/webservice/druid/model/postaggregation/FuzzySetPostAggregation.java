// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Model representing post aggregation numeric estimates from sketches.
 */
public abstract class FuzzySetPostAggregation extends PostAggregation implements WithPostAggregations {

    protected final PostAggregation field;

    /**
     * Constructor accepting fields as list of field accessor post aggs.
     *
     * @param type  The post aggregation type descriptor
     * @param name  The name of post aggregation
     * @param field  The list of field accessor post aggs
     */
    public FuzzySetPostAggregation(PostAggregationType type, String name, PostAggregation field) {
        super(type, name);
        this.field = field;
    }

    public PostAggregation getField() {
        return field;
    }

    @JsonIgnore
    @Override
    public List<PostAggregation> getPostAggregations() {
        return Collections.singletonList(getField());
    }

    @JsonIgnore
    public PostAggregationType getPostAggregationType() {
        return getType();
    }

    @Override
    public abstract FuzzySetPostAggregation withName(String name);

    /**
     * Creates a new FuzzySetPostAggregation with the provided field.
     *
     * @param field  Field for the new aggregation
     *
     * @return a new instance with the given field
     */
    public abstract FuzzySetPostAggregation withField(PostAggregation field);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FuzzySetPostAggregation)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        FuzzySetPostAggregation that = (FuzzySetPostAggregation) o;

        return
                Objects.equals(field, that.field) &&
                Objects.equals(getType(), that.getType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field, getType());
    }
}
