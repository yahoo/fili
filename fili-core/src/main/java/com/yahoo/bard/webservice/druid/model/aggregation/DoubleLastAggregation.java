// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

/**
 * Aggregation that computes the metric value with the maximum timestamp for doubles.
 */
public class DoubleLastAggregation extends Aggregation {

    /**
     * Constructor.
     *
     * @param name  The name of the metric value with the maximum timestamp
     * @param fieldName  The name of the metric whose maximum timestamp is to be calculated
     */
    public DoubleLastAggregation(String name, String fieldName) {
        super(name, fieldName);
    }

    @Override
    public String getType() {
        return "doubleLast";
    }

    @Override
    public DoubleLastAggregation withName(String name) {
        return new DoubleLastAggregation(name, getFieldName());
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new DoubleLastAggregation(getName(), fieldName);
    }
}
