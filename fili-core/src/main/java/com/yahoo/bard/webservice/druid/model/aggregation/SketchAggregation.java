// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation;

/**
 * Base type for sketch aggregations.
 */
public abstract class SketchAggregation extends Aggregation {

    private final int size;

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof SketchAggregation)) { return false; }
        if (!super.equals(o)) { return false; }

        SketchAggregation that = (SketchAggregation) o;

        if (size != that.size) { return false; }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + size;
        return result;
    }

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
}
