// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

/**
 * Aggregation that computes the metric value with the any timestamp for floats.
 */
public class FloatAnyAggregation extends Aggregation {

    /**
     * Constructor.
     *
     * @param name  The name of the metric value with the any timestamp
     * @param fieldName  The name of the metric whose any timestamp is to be calculated
     */
    public FloatAnyAggregation(String name, String fieldName) {
        super(name, fieldName);
    }

    @Override
    public String getType() {
        return "floatAny";
    }

    @Override
    public FloatAnyAggregation withName(String name) {
        return new FloatAnyAggregation(name, getFieldName());
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new FloatAnyAggregation(getName(), fieldName);
    }
}
