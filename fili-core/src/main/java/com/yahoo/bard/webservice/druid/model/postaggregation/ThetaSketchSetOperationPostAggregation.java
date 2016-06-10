// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.THETA_SKETCH_SET_OP;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Arrays;
import java.util.List;

/**
 * Model representing a post aggregation of sketch set operations.
 */
public class ThetaSketchSetOperationPostAggregation extends PostAggregation
        implements WithFields<ThetaSketchSetOperationPostAggregation> {

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

    /**
     * Constructor accepting two post aggregations as fields while leaving the sketch size of the resulting
     * postaggregation undefined.
     *
     * @param name  The name of the post aggregation
     * @param func  The func of the post aggregation
     * @param field1  post aggregation
     * @param field2  post aggregation
     */
    public ThetaSketchSetOperationPostAggregation(
            String name,
            SketchSetOperationPostAggFunction func,
            PostAggregation field1,
            PostAggregation field2
    ) {
        this(name, func, Arrays.asList(field1, field2), null);
    }

    /**
     * Constructor accepting two post aggregations as fields as well as an explicit sketch size.
     *
     * @param name  The name of the post aggregation
     * @param func  The func of the post aggregation
     * @param field1  post aggregation
     * @param field2  post aggregation
     * @param size  sketch size of the post aggregation
     */
    public ThetaSketchSetOperationPostAggregation(
            String name,
            SketchSetOperationPostAggFunction func,
            PostAggregation field1,
            PostAggregation field2,
            Integer size
    ) {
        this(name, func, Arrays.asList(field1, field2), size);
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
        return "Aggregation{type=" + getType() + ", name=" + name + ", func=" + func + ", fields=" + fields +
                ", size=" + size + "}";
    }

    @Override
    public ThetaSketchSetOperationPostAggregation withName(String name) {
        return new ThetaSketchSetOperationPostAggregation(name, func, fields, size);
    }

    public ThetaSketchSetOperationPostAggregation withFunc(SketchSetOperationPostAggFunction func) {
        return new ThetaSketchSetOperationPostAggregation(name, func, fields, size);
    }

    public ThetaSketchSetOperationPostAggregation withSize(int size) {
        return new ThetaSketchSetOperationPostAggregation(name, func, fields, size);
    }

    @Override
    public ThetaSketchSetOperationPostAggregation withFields(List<PostAggregation> fields) {
        return new ThetaSketchSetOperationPostAggregation(name, func, fields, size);
    }

    @Override
    public boolean isSketch() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof ThetaSketchSetOperationPostAggregation)) { return false; }
        if (!super.equals(o)) { return false; }

        ThetaSketchSetOperationPostAggregation that = (ThetaSketchSetOperationPostAggregation) o;

        if (fields != null ? !fields.equals(that.fields) : that.fields != null) { return false; }
        if (func != that.func) { return false; }
        if (size != null ? !size.equals(that.size) : that.size != null) { return false; }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (func != null ? func.hashCode() : 0);
        result = 31 * result + (size != null ? size.hashCode() : 0);
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        return result;
    }
}
