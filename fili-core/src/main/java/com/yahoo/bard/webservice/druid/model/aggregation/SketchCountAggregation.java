// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Aggregation for the Sketch Count custom sketches aggregation.
 * <p>
 * Sketch count merges all the sketches while aggregating and then returns the cardinality of the set.
 *
 * @deprecated  To consider the latest version of sketch Library.
 * This class is replaced by ThetaSketchAggregation
 */
@Deprecated
public class SketchCountAggregation extends SketchAggregation {

    private static final String AGGREGATION_TYPE = "sketchCount";

    public SketchCountAggregation(String name, String fieldName, int size) {
        super(name, fieldName, size);
    }

    @Override
    public String getType() {
        return AGGREGATION_TYPE;
    }

    @Override
    public SketchCountAggregation withName(String name) {
        return new SketchCountAggregation(name, getFieldName(), getSize());
    }

    @Override
    public SketchCountAggregation withFieldName(String fieldName) {
        return new SketchCountAggregation(getName(), fieldName, getSize());
    }

    @Override
    public SketchCountAggregation withSize(int size) {
        return new SketchCountAggregation(getName(), getFieldName(), size);
    }

    @Override
    public Pair<Aggregation, Aggregation> nest() {
        String nestingFieldName = getName();

        // In order to maintain the correct meaning of the sketch count, the sketch merge must nest at all lower levels
        SketchMergeAggregation innerMerge = new SketchMergeAggregation(nestingFieldName, getFieldName(), getSize());
        SketchCountAggregation outerCount = this.withFieldName(nestingFieldName);

        return new ImmutablePair<>(outerCount, innerMerge);
    }

    @Override
    public boolean isSketch() {
        return true;
    }
}
