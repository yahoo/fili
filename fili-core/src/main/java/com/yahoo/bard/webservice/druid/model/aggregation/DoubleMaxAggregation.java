// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

/**
 * Aggregation for the maximum of doubles.
 */
public class DoubleMaxAggregation extends Aggregation {

    /**
     * Constructor.
     *
     * @param name  The name of the maximum
     * @param fieldName  The name of the metric whose max is to be calculated
     */
    public DoubleMaxAggregation(String name, String fieldName) {
        super(name, fieldName);
    }

    @Override
    public String getType() {
        return "doubleMax";
    }

    @Override
    public DoubleMaxAggregation withName(String name) {
        return new DoubleMaxAggregation(name, getFieldName());
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new DoubleMaxAggregation(getName(), fieldName);
    }
}
