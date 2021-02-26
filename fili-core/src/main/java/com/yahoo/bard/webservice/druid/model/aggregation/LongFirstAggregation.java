// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

/**
 * Aggregation that computes the metric value with the minimum timestamp for longs.
 */
public class LongFirstAggregation extends Aggregation {

    /**
     * Constructor.
     *
     * @param name  The name of the metric value with the minimum timestamp
     * @param fieldName  The name of the metric whose minimum timestamp is to be calculated
     */
    public LongFirstAggregation(String name, String fieldName) {
        super(name, fieldName);
    }

    @Override
    public String getType() {
        return "longFirst";
    }

    @Override
    public LongFirstAggregation withName(String name) {
        return new LongFirstAggregation(name, getFieldName());
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new LongFirstAggregation(getName(), fieldName);
    }
}
