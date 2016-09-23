// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

/**
 * Aggregation for the minimum of doubles.
 */
public class DoubleMinAggregation extends Aggregation {

    /**
     * Constructor.
     *
     * @param name  The name of the minimum
     * @param fieldName  The name of the metric whose min is to be calculated
     */
    public DoubleMinAggregation(String name, String fieldName) {
        super(name, fieldName);
    }

    @Override
    public String getType() {
        return "doubleMin";
    }

    @Override
    public DoubleMinAggregation withName(String name) {
        return new DoubleMinAggregation(name, getFieldName());
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new DoubleMinAggregation(getName(), fieldName);
    }
}
