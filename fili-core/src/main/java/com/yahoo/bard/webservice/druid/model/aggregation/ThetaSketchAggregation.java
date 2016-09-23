// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

/**
 * Aggregation for custom sketches aggregations.
 */
public class ThetaSketchAggregation extends SketchAggregation {

    private static final String AGGREGATION_TYPE = "thetaSketch";

    /**
     * Constructor.
     *
     * @param name  Name of the aggregation
     * @param fieldName  Name of the column that this aggregation is aggregating over
     * @param size  Since of the sketch to use
     */
    public ThetaSketchAggregation(String name, String fieldName, int size) {
        super(name, fieldName, size);
    }

    @Override
    public String getType() {
        return AGGREGATION_TYPE;
    }

    @Override
    public ThetaSketchAggregation withName(String name) {
        return new ThetaSketchAggregation(name, getFieldName(), getSize());
    }

    @Override
    public ThetaSketchAggregation withFieldName(String fieldName) {
        return new ThetaSketchAggregation(getName(), fieldName, getSize());
    }

    @Override
    public ThetaSketchAggregation withSize(int size) {
        return new ThetaSketchAggregation(getName(), getFieldName(), size);
    }

    @Override
    public boolean isSketch() {
        return true;
    }
}
