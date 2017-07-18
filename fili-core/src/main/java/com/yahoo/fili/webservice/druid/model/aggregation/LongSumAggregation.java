// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.druid.model.aggregation;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Aggregation for the sum of longs.
 */
public class LongSumAggregation extends Aggregation {

    /**
     * Constructor.
     *
     * @param name  Name of the aggregation
     * @param fieldName  Name of the column that this aggregation is aggregating over
     */
    public LongSumAggregation(String name, String fieldName) {
        super(name, fieldName);
    }

    @Override
    public String getType() {
        return "longSum";
    }

    @Override
    public LongSumAggregation withName(String name) {
        return new LongSumAggregation(name, getFieldName());
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new LongSumAggregation(getName(), fieldName);
    }

    @JsonIgnore
    public boolean isFloatingPoint() {
        return false;
    }
}
