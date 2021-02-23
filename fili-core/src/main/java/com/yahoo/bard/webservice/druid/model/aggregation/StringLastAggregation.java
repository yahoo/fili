// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

/**
 * Aggregation that computes the metric value with the maximum timestamp for strings.
 */
public class StringLastAggregation extends Aggregation {

    private final Integer maxStringBytes;

    /**
     * Constructor.
     *
     * @param name  The name of the metric value with the maximum timestamp
     * @param fieldName  The name of the metric whose maximum timestamp is to be calculated
     */
    public StringLastAggregation(String name, String fieldName) {
        this(name, fieldName, new Integer(1024));
    }

    /**
     * Constructor.
     *
     * @param name  The name of the metric value with the maximum timestamp
     * @param fieldName  The name of the metric whose maximum timestamp is to be calculated
     * @param maxStringBytes The max bytes of the string metric
     */
    public StringLastAggregation(String name, String fieldName, Integer maxStringBytes) {
        super(name, fieldName);
        this.maxStringBytes = maxStringBytes;
    }

    @Override
    public String getType() {
        return "stringLast";
    }

    @Override
    public StringLastAggregation withName(String name) {
        return new StringLastAggregation(name, getFieldName());
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new StringLastAggregation(getName(), fieldName);
    }

    public Aggregation withMaxBytes(Integer maxStringBytes) {
        return new StringLastAggregation(getName(), getFieldName(), maxStringBytes);
    }
}
