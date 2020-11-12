// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.THETA_SKETCH_SET_OP;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Model representing a post aggregation of sketch set operations.
 */
public class ThetaSketchSetOperationPostAggregation extends PostAggregation implements WithPostAggregations {

    private final SketchSetOperationPostAggFunction func;
    private final List<PostAggregation> fields;
    private final Integer size;

    /**
     * Constructor accepting a list of post aggregations as fields  as well as an explicit sketch size.
     *
     * @param name  The name of the post aggregation
     * @param func  The func of the post aggregation
     * @param fields  list of post aggregations
     * @param size  sketch size of the post aggregation
     */
    public ThetaSketchSetOperationPostAggregation(
            String name,
            SketchSetOperationPostAggFunction func,
            List<PostAggregation> fields,
            Integer size
    ) {
        super(THETA_SKETCH_SET_OP, name);
        this.func = func;
        this.fields = fields;
        this.size = size;
    }

    /**
     * Constructor accepting a list of post aggregations as fields while leaving the sketch size of the resulting
     * postaggregation undefined.
     *
     * @param name  The name of the post aggregation
     * @param func  The func of the post aggregation
     * @param fields  list of post aggregations
     */
    public ThetaSketchSetOperationPostAggregation(
            String name,
            SketchSetOperationPostAggFunction func,
            List<PostAggregation> fields
    ) {
        this(name, func, fields, null);
    }

    public SketchSetOperationPostAggFunction getFunc() {
        return func;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getSize() {
        return size;
    }

    @Override
    public List<PostAggregation> getPostAggregations() {
        return fields;
    }

    @Override
    public String toString() {
        return "Aggregation{type=" + getType() + ", name=" + getName() + ", func=" + getFunc() + ", fields=" +
                getPostAggregations() + ", size=" + getSize() + "}";
    }

    @Override
    public ThetaSketchSetOperationPostAggregation withName(String name) {
        return new ThetaSketchSetOperationPostAggregation(name, getFunc(), getPostAggregations(), getSize());
    }

    /**
     * Get a new instance of this PostAggregation with the given func.
     *
     * @param func  Function of the new PostAggregation.
     *
     * @return a new instance of this with the given function
     */
    public ThetaSketchSetOperationPostAggregation withFunc(SketchSetOperationPostAggFunction func) {
        return new ThetaSketchSetOperationPostAggregation(getName(), func, getPostAggregations(), getSize());
    }

    /**
     * Get a new instance of this PostAggregation with the given size.
     *
     * @param size  Size of the new PostAggregation.
     *
     * @return a new instance of this with the given size
     */
    public ThetaSketchSetOperationPostAggregation withSize(int size) {
        return new ThetaSketchSetOperationPostAggregation(getName(), getFunc(), getPostAggregations(), size);
    }

    /**
     * Get a new instance of this PostAggregation with the given fields.
     *
     * @param fields  Fields of the new PostAggregation.
     *
     * @return a new instance of this with the given fields
     */
    @Override
    public ThetaSketchSetOperationPostAggregation withPostAggregations(List<? extends PostAggregation> fields) {
        return new ThetaSketchSetOperationPostAggregation(getName(), getFunc(), new ArrayList<>(fields), getSize());
    }

    @Override
    public boolean isSketch() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof ThetaSketchSetOperationPostAggregation)) { return false; }
        ThetaSketchSetOperationPostAggregation that = (ThetaSketchSetOperationPostAggregation) o;
        return
                super.equals(o) &&
                Objects.equals(fields, that.fields) &&
                Objects.equals(func, that.func) &&
                Objects.equals(size, that.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), func, size, fields);
    }
}
