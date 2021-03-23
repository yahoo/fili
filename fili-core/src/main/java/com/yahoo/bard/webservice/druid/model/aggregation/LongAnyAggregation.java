// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

/**
 * Aggregation that computes the metric value with the any timestamp for longs.
 */
public class LongAnyAggregation extends Aggregation {

    /**
     * Constructor.
     *
     * @param name  The name of the metric value with the any timestamp
     * @param fieldName  The name of the metric whose any timestamp is to be calculated
     */
    public LongAnyAggregation(String name, String fieldName) {
        super(name, fieldName);
    }

    @Override
    public String getType() {
        return "longAny";
    }

    @Override
    public LongAnyAggregation withName(String name) {
        return new LongAnyAggregation(name, getFieldName());
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new LongAnyAggregation(getName(), fieldName);
    }
}
