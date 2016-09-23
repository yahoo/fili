// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

/**
 * Aggregation for min.
 * <p>
 * Druid now supports separate min and max operations for Long and Double respectively. So {@link LongMinAggregation}
 * and {@link DoubleMinAggregation} should be used instead.
 *
 * @deprecated in favor of LongMinAggregation, since Druid is deprecating MinAggregation
 */
@Deprecated
public class MinAggregation extends Aggregation {

    /**
     * Constructor.
     *
     * @param name  Name of the aggregation
     * @param fieldName  Name of the column that this aggregation is aggregating over
     */
    public MinAggregation(String name, String fieldName) {
        super(name, fieldName);
    }

    @Override
    public String getType() {
        return "min";
    }

    @Override
    public MinAggregation withName(String name) {
        return new MinAggregation(name, getFieldName());
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new MinAggregation(getName(), fieldName);
    }
}
