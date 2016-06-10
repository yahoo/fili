// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.ARITHMETIC;

import com.yahoo.bard.webservice.druid.model.HasDruidName;
import com.yahoo.bard.webservice.druid.serializers.HasDruidNameSerializer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Model representing arithmetic post aggregations.
 */
public class ArithmeticPostAggregation extends PostAggregation implements WithFields<ArithmeticPostAggregation> {

    private final ArithmeticPostAggregationFunction fn;

    private final List<PostAggregation> fields;

    private final boolean floatingPoint;

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof ArithmeticPostAggregation)) { return false; }
        if (!super.equals(o)) { return false; }

        ArithmeticPostAggregation that = (ArithmeticPostAggregation) o;

        if (fields != null ? !fields.equals(that.fields) : that.fields != null) { return false; }
        if (fn != that.fn) { return false; }

        return true;
    }

    @Override
    public String toString() {
        return "ArithmeticPostAggregation{" + "name=" + name + "," + "fn=" + fn + ", fields=" + fields + '}';
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (fn != null ? fn.hashCode() : 0);
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        return result;
    }

    public ArithmeticPostAggregation(String name, ArithmeticPostAggregationFunction fn, List<PostAggregation> fields) {
        super(ARITHMETIC, name);
        this.fn = fn;
        if (fn.equals(ArithmeticPostAggregationFunction.DIVIDE)) {
            this.floatingPoint = true;
        } else {
            boolean fp = false;
            for (PostAggregation pa: fields) {
                fp = fp || pa.isFloatingPoint();
            }
            this.floatingPoint = fp;
        }
        this.fields = new ArrayList<>(fields);
    }

    public ArithmeticPostAggregation(
            String name,
            ArithmeticPostAggregationFunction fn,
            PostAggregation field1,
            PostAggregation field2
    ) {
        this(name, fn, Arrays.asList(field1, field2));
    }

    @JsonSerialize(using = HasDruidNameSerializer.class)
    public ArithmeticPostAggregationFunction getFn() {
        return fn;
    }

    @Override
    public List<PostAggregation> getFields() {
        return new ArrayList<>(fields);
    }

    @Override
    public ArithmeticPostAggregation withName(String name) {
        return new ArithmeticPostAggregation(name, fn, fields);
    }

    public ArithmeticPostAggregation withFn(ArithmeticPostAggregationFunction fn) {
        return new ArithmeticPostAggregation(getName(), fn, fields);
    }

    @Override
    public ArithmeticPostAggregation withFields(List<PostAggregation> fields) {
        return new ArithmeticPostAggregation(getName(), fn, fields);
    }

    /**
     * The defined arithmetic functions for post aggregation
     */
    public enum ArithmeticPostAggregationFunction implements HasDruidName {
        PLUS("+"),
        MINUS("-"),
        MULTIPLY("*"),
        DIVIDE("/");

        private final String druidName;

        ArithmeticPostAggregationFunction(String druidName) {
            this.druidName = druidName;
        }

        @Override
        public String getDruidName() {
            return druidName;
        }
    }

    @Override
    @JsonIgnore
    public boolean isFloatingPoint() {
        return floatingPoint;
    }
}
