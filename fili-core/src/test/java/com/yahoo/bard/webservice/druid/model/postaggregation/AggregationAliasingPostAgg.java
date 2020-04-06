// Copyright 2020 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Test class that represents a (currently non existant) PostAgg that aliases an Aggregation to a different output name.
 */
public class AggregationAliasingPostAgg extends PostAggregation
        implements AggregationReference<AggregationAliasingPostAgg> {

    private Aggregation targetAgg;

    public AggregationAliasingPostAgg(String outputName, Aggregation targetAgg) {
        super(DefaultPostAggregationType.FIELD_ACCESS, outputName);
        this.targetAgg = targetAgg;
    }

    @Override
    public List<Aggregation> getAggregations() {
        return Collections.singletonList(targetAgg);
    }

    @Override
    public AggregationAliasingPostAgg withAggregations(List<Aggregation> aggregations) {
        return new AggregationAliasingPostAgg(getName(), aggregations.get(0));
    }

    @Override
    public PostAggregation withName(String name) {
        return new AggregationAliasingPostAgg(name, getAggregations().get(0));
    }

    @Override
    public Set<Dimension> getDependentDimensions() {
        return Collections.emptySet();
    }

    public Aggregation getTargetAgg() {
        return targetAgg;
    }

    public void setTargetAgg(Aggregation targetAgg) {
        this.targetAgg = targetAgg;
    }
}
