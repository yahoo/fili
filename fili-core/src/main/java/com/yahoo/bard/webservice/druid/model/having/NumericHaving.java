// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.having;

import com.yahoo.bard.webservice.druid.serializers.HasDruidNameSerializer;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Having clause model for numeric matching expressions.
 */
public class NumericHaving extends Having {

    private final String aggregation;

    // Allows us to support integers and floating point without having to cast one to the other, or treating them
    // separately.
    private final Number value;

    /**
     * Constructor.
     *
     * @param type  The type of having operation requested (i.e. AND, GREATER_THAN, etc).
     * @param aggregation  The name of the aggregation being performed.
     * @param value  The value to be compared against.
     */
    public NumericHaving(HavingType type, String aggregation, Number value) {
        super(type);
        this.aggregation = aggregation;
        this.value = value;
    }

    @Override
    @JsonSerialize(using = HasDruidNameSerializer.class)
    public HavingType getType() {
        return super.getType();
    }

    public String getAggregation() {
        return aggregation;
    }

    public Number getValue() {
        return value;
    }

    /**
     * Get a new instance of this having with the given type.
     *
     * @param type  Type of the new having.
     *
     * @return a new instance of this having with the given type
     */
    public NumericHaving withType(HavingType type) {
        return new NumericHaving(type, getAggregation(), getValue());
    }

    /**
     * Get a new instance of this having with the given aggregation.
     *
     * @param aggregation  Aggregation of the new having.
     *
     * @return a new instance of this having with the given aggregation
     */
    public NumericHaving withAggregation(String aggregation) {
        return new NumericHaving(getType(), aggregation, getValue());
    }

    /**
     * Get a new instance of this having with the given value.
     *
     * @param value  Value of the new having.
     *
     * @return a new instance of this having with the given value
     */
    public NumericHaving withValue(Number value) {
        return new NumericHaving(getType(), getAggregation(), value);
    }
}
