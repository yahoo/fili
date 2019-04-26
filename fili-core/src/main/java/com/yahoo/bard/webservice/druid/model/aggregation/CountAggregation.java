// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Optional;

/**
 * Aggregation for counts.
 */
public class CountAggregation extends Aggregation {

    /**
     * Constructor.
     *
     * @param name  Name of the aggregation.
     */
    public CountAggregation(String name) {
        super(name, "");
    }

    @JsonIgnore
    @Override
    public String getFieldName() {
        return super.getFieldName();
    }

    @Override
    public String getType() {
        return "count";
    }

    @Override
    public CountAggregation withName(String name) {
        return new CountAggregation(name);
    }

    @Override
    public Aggregation withFieldName(String fieldName) {
        return new CountAggregation(getName());
    }

    /**
     * Nesting a 'count' aggregation changes the outer aggregation
     * to a 'longSum' whereas the inner aggregation remains unchanged.
     * Base class aggregation transformation is also performed.
     */
    @Override
    public Pair<Optional<Aggregation>, Optional<Aggregation>> nest() {
        String nestingName = getName();
        Aggregation inner = withName(nestingName);
        Aggregation outer = new LongSumAggregation(getName(), nestingName);
        return new ImmutablePair<>(Optional.of(outer), Optional.of(inner));
    }

    @JsonIgnore
    public boolean isFloatingPoint() {
        return false;
    }
}
