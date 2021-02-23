// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

/**
 * Aggregation that computes the metric value with any timestamp for strings.
 */
public class StringAnyAggregation extends Aggregation {

    private final Integer maxStringBytes;

    /**
     * Constructor.
     *
     * @param name  The name of the metric value with any timestamp
     * @param fieldName  The name of the metric whose any timestamp is to be calculated
     */
    public StringAnyAggregation(String name, String fieldName) {
        this(name, fieldName, new Integer(1024));
    }

    /**
     * Constructor.
     *
     * @param name  The name of the metric value with any timestamp
     * @param fieldName  The name of the metric whose any timestamp is to be calculated
     * @param maxStringBytes The max bytes of the string metric
     */
    public StringAnyAggregation(String name, String fieldName, Integer maxStringBytes) {
        super(name, fieldName);
        this.maxStringBytes = maxStringBytes;
    }

    @Override
    public String getType() {
        return "stringAny";
    }

    @Override
    public StringAnyAggregation withName(String name) {
        return new StringAnyAggregation(name, getFieldName());
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new StringAnyAggregation(getName(), fieldName);
    }

    public Aggregation withMaxBytes(Integer maxStringBytes) {
        return new StringAnyAggregation(getName(), getFieldName(), maxStringBytes);
    }
}
