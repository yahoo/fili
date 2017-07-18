// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.druid.model.postaggregation;

import static com.yahoo.fili.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.SKETCH_SET_OPER;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;

/**
 * Model representing a post aggregation of sketch set operations.
 *
 * @deprecated  To consider the latest version of sketch Library.
 * This class is replaced by ThetaSketchSetOperationPostAggregation class
 */
@Deprecated
public class SketchSetOperationPostAggregation extends PostAggregation
        implements WithFields<SketchSetOperationPostAggregation> {

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
    public SketchSetOperationPostAggregation(
            String name,
            SketchSetOperationPostAggFunction func,
            List<PostAggregation> fields,
            Integer size
    ) {
        super(SKETCH_SET_OPER, name);
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
    public SketchSetOperationPostAggregation(
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
    public List<PostAggregation> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        return "Aggregation{type=" + getType() + ", name=" + getName() + ", func=" + getFunc() + ", fields=" +
                getFields() + ", size=" + getSize() + "}";
    }

    @Override
    public SketchSetOperationPostAggregation withName(String name) {
        return new SketchSetOperationPostAggregation(name, getFunc(), getFields(), getSize());
    }

    /**
     * Get a new instance of this PostAggregation with the given func.
     *
     * @param func  Function of the new PostAggregation.
     *
     * @return a new instance of this with the given function
     */
    public SketchSetOperationPostAggregation withFunc(SketchSetOperationPostAggFunction func) {
        return new SketchSetOperationPostAggregation(getName(), func, getFields(), getSize());
    }

    /**
     * Get a new instance of this PostAggregation with the given size.
     *
     * @param size  Size of the new PostAggregation.
     *
     * @return a new instance of this with the given size
     */
    public SketchSetOperationPostAggregation withSize(int size) {
        return new SketchSetOperationPostAggregation(getName(), getFunc(), getFields(), size);
    }

    /**
     * Get a new instance of this PostAggregation with the given fields.
     *
     * @param fields  Fields of the new PostAggregation.
     *
     * @return a new instance of this with the given fields
     */
    @Override
    public SketchSetOperationPostAggregation withFields(List<PostAggregation> fields) {
        return new SketchSetOperationPostAggregation(getName(), getFunc(), fields, getSize());
    }

    @Override
    public boolean isSketch() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof SketchSetOperationPostAggregation)) { return false; }
        SketchSetOperationPostAggregation that = (SketchSetOperationPostAggregation) o;
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
