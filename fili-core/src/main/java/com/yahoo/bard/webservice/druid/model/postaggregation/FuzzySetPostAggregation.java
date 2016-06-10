// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Model representing post aggregation numeric estimates from sketches.
 */
public abstract class FuzzySetPostAggregation extends PostAggregation implements WithFields<FuzzySetPostAggregation> {

    protected final PostAggregation field;
    private static final Logger LOG = LoggerFactory.getLogger(FuzzySetPostAggregation.class);

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
    public PostAggregationType getPostAggregationType() {
        return type;
    }

    @JsonIgnore
    @Override
    public List<PostAggregation> getFields() {
        return Collections.singletonList(this.getField());
    }

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

        if (field != null ? !field.equals(that.field) : that.field != null) {
            return false;
        }

        if (type != null ? !type.equals(that.type) : that.type != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (field != null ? field.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }

    @Override
    public abstract FuzzySetPostAggregation withName(String name);

    public abstract FuzzySetPostAggregation withField(FieldAccessorPostAggregation field);

    /**
     * SketchEstimate converts the sketch into a number. Hence this method always should have one aggregator
     *
     * @param fields  List of post aggregation fields
     *
     * @return New SketchEstimatePostAggregation with provided field and only one aggregator.
     */
    @JsonIgnore
    @Override
    public abstract FuzzySetPostAggregation withFields(List<PostAggregation> fields);
}
