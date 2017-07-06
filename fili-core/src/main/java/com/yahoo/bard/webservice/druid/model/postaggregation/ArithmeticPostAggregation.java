// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.ARITHMETIC;

import com.yahoo.bard.webservice.druid.model.HasDruidName;
import com.yahoo.bard.webservice.druid.serializers.HasDruidNameSerializer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Model representing arithmetic post aggregations.
 */
public class ArithmeticPostAggregation extends PostAggregation implements WithFields<ArithmeticPostAggregation> {

    private final ArithmeticPostAggregationFunction fn;

    private final List<PostAggregation> fields;

    private final boolean floatingPoint;

    /**
     * Constructor.
     *
     * @param name  Name of the post-aggregation
     * @param fn  Arithmetic function the post-aggregation should apply
     * @param fields  Fields the post-aggregation will operate on
     */
    public ArithmeticPostAggregation(String name, ArithmeticPostAggregationFunction fn, List<PostAggregation> fields) {
        super(ARITHMETIC, name);
        this.fn = fn;
        this.fields = Collections.unmodifiableList(fields);
        this.floatingPoint = fn.equals(ArithmeticPostAggregationFunction.DIVIDE) ||
                fields.stream().anyMatch(PostAggregation::isFloatingPoint);
    }

    @JsonSerialize(using = HasDruidNameSerializer.class)
    public ArithmeticPostAggregationFunction getFn() {
        return fn;
    }

    @Override
    public List<PostAggregation> getFields() {
        return fields;
    }

    @Override
    @JsonIgnore
    public boolean isFloatingPoint() {
        return floatingPoint;
    }

    @Override
    public ArithmeticPostAggregation withName(String name) {
        return new ArithmeticPostAggregation(name, getFn(), getFields());
    }

    /**
     * Get a new instance of this with the given function.
     *
     * @param fn  Function of the new instance.
     *
     * @return a new instance of this with the given function
     */
    public ArithmeticPostAggregation withFn(ArithmeticPostAggregationFunction fn) {
        return new ArithmeticPostAggregation(getName(), fn, getFields());
    }

    @Override
    public ArithmeticPostAggregation withFields(List<PostAggregation> fields) {
        return new ArithmeticPostAggregation(getName(), getFn(), fields);
    }

    @Override
    public String toString() {
        return "ArithmeticPostAggregation{name=" + getName() + ", fn=" + getFn() + ", fields=" + getFields() + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fn, fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof ArithmeticPostAggregation)) { return false; }
        ArithmeticPostAggregation that = (ArithmeticPostAggregation) o;
        return
                super.equals(o) &&
                Objects.equals(fields, that.fields) &&
                fn == that.fn;
    }

    /**
     * The defined arithmetic functions for post aggregation.
     */
    public enum ArithmeticPostAggregationFunction implements HasDruidName {
        PLUS("+"),
        MINUS("-"),
        MULTIPLY("*"),
        DIVIDE("/");

        private final String druidName;

        /**
         * Constructor.
         *
         * @param druidName  Druid name of the function
         */
        ArithmeticPostAggregationFunction(String druidName) {
            this.druidName = druidName;
        }

        @Override
        public String getDruidName() {
            return druidName;
        }

        public static ArithmeticPostAggregationFunction fromDruidName(String druidName) {
            for (ArithmeticPostAggregationFunction fn : values()) {
                if (fn.druidName.equals(druidName)) {
                    return fn;
                }
            }
            return null;
        }
    }
}
