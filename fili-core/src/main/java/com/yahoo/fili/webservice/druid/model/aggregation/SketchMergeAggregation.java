// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.druid.model.aggregation;

/**
 * Aggregation for the Sketch Merge custom sketches aggregation.
 * <p>
 * Sketch merge performs unions on all sketches from aggregated rows.
 *
 * @deprecated  To consider the latest version of sketch Library.
 * This class is replaced by ThetaSketchAggregation
 */
@Deprecated
public class SketchMergeAggregation extends SketchAggregation {

    private static final String AGGREGATION_TYPE = "sketchMerge";

    /**
     * Constructor.
     *
     * @param name  Name of the aggregation
     * @param fieldName  Name of the column that this aggregation is aggregating over
     * @param size  Since of the sketch to use
     */
    public SketchMergeAggregation(String name, String fieldName, int size) {
        super(name, fieldName, size);
    }

    @Override
    public String getType() {
        return AGGREGATION_TYPE;
    }

    @Override
    public SketchMergeAggregation withName(String name) {
        return new SketchMergeAggregation(name, getFieldName(), getSize());
    }

    @Override
    public SketchMergeAggregation withFieldName(String fieldName) {
        return new SketchMergeAggregation(getName(), fieldName, getSize());
    }

    @Override
    public SketchMergeAggregation withSize(int size) {
        return new SketchMergeAggregation(getName(), getFieldName(), size);
    }

    @Override
    public boolean isSketch() {
        return true;
    }
}
