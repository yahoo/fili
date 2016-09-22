// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

import java.util.Objects;

/**
 * Base type for sketch aggregations.
 */
public abstract class SketchAggregation extends Aggregation {

    private final int size;

    /**
     * Constructor.
     *
     * @param name  Name of the aggregation
     * @param fieldName  Name of the column that this aggregation is aggregating over
     * @param size  Size of the sketch to use
     */
    public SketchAggregation(String name, String fieldName, int size) {
        super(name, fieldName);
        this.size = size;
    }

    public int getSize() {
        return size;
    }

    /**
     * Creates a SketchAggregation with the provided bucketSize.
     *
     * @param size  Size for the new SketchAggregation
     *
     * @return a new subclass of SketchAggregation.
     */
    public abstract SketchAggregation withSize(int size);

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof SketchAggregation)) { return false; }

        SketchAggregation that = (SketchAggregation) o;

        return
                super.equals(o) &&
                size == that.size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), size);
    }
}
