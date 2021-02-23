// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

/**
 * Aggregation that computes the metric value with the minimum timestamp for strings.
 */
public class StringFirstAggregation extends Aggregation {

    private final Integer maxStringBytes;

    /**
     * Constructor.
     *
     * @param name  The name of the metric value with the minimum timestamp
     * @param fieldName  The name of the metric whose minimum timestamp is to be calculated
     */
    public StringFirstAggregation(String name, String fieldName) {
        this(name, fieldName, new Integer(1024));
    }

    /**
     * Constructor.
     *
     * @param name  The name of the metric value with the minimum timestamp
     * @param fieldName  The name of the metric whose minimum timestamp is to be calculated
     * @param maxStringBytes The max bytes of the string metric
     */
    public StringFirstAggregation(String name, String fieldName, Integer maxStringBytes) {
        super(name, fieldName);
        this.maxStringBytes = maxStringBytes;
    }

    @Override
    public String getType() {
        return "stringFirst";
    }

    @Override
    public StringFirstAggregation withName(String name) {
        return new StringFirstAggregation(name, getFieldName());
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new StringFirstAggregation(getName(), fieldName);
    }

    public Aggregation withMaxBytes(Integer maxStringBytes) {
        return new StringFirstAggregation(getName(), getFieldName(), maxStringBytes);
    }
}
