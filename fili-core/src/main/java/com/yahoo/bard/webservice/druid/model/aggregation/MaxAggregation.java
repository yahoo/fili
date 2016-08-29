// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

/**
 * Aggregation for the max.
 * <p>
 * Druid now supports separate min and max operations for Long and Double respectively. So {@link LongMinAggregation}
 * and {@link DoubleMinAggregation} should be used instead.
 *
 * @deprecated in favor of LongMaxAggregation or DoubleMaxAggregation, since Druid is deprecating MaxAggregation
 */
@Deprecated
public class MaxAggregation extends Aggregation {

    /**
     * Constructor.
     *
     * @param name  Name of the aggregation
     * @param fieldName  Name of the column that this aggregation is aggregating over
     */
    public MaxAggregation(String name, String fieldName) {
        super(name, fieldName);
    }

    @Override
    public String getType() {
        return "max";
    }

    @Override
    public MaxAggregation withName(String name) {
        return new MaxAggregation(name, getFieldName());
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new MaxAggregation(getName(), fieldName);
    }
}
