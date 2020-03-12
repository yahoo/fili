// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

/**
 * Aggregation for the minimum of longs.
 */
public class LongMinAggregation extends Aggregation {

    /**
     * Constructor.
     *
     * @param name  The name of the minimum
     * @param fieldName  The name of the metric whose min is to be calculated
     */
    public LongMinAggregation(String name, String fieldName) {
        super(name, fieldName);
    }

    @Override
    public String getType() {
        return "longMin";
    }

    @Override
    public LongMinAggregation withName(String name) {
        return new LongMinAggregation(name, getFieldName());
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new LongMinAggregation(getName(), fieldName);
    }

    @Override
    public boolean isFloatingPoint() {
        return false;
    }
}
