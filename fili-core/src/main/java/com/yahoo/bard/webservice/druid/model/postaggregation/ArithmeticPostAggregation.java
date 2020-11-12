// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.ARITHMETIC;

import com.yahoo.bard.webservice.druid.model.HasDruidName;
import com.yahoo.bard.webservice.druid.serializers.HasDruidNameSerializer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Model representing arithmetic post aggregations.
 */
public class ArithmeticPostAggregation extends PostAggregation implements WithPostAggregations {

    private final ArithmeticPostAggregationFunction fn;

    private final List<PostAggregation> fields;

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
    }

    @JsonSerialize(using = HasDruidNameSerializer.class)
    public ArithmeticPostAggregationFunction getFn() {
        return fn;
    }

    // CHECKSTYLE:OFF

    /**
     * Druid ALWAYS coerces result of arithmetic post aggregations to Double. See:
     * https://github.com/apache/druid/blob/master/processing/src/main/java/org/apache/druid/query/aggregation/post
     * /ArithmeticPostAggregator.java#L106-L126
     * Relevant Druid class: {@code org.apache.druid.query.aggregation.post.ArithmeticPostAggregator}
     *
     * @inheritDoc
     */
    @Override
    @JsonIgnore
    public boolean isFloatingPoint() {
        return true;
    }
    // CHECKSTYLE:ON

    @Override
    public ArithmeticPostAggregation withName(String name) {
        return new ArithmeticPostAggregation(name, getFn(), getPostAggregations());
    }

    /**
     * Get a new instance of this with the given function.
     *
     * @param fn  Function of the new instance.
     *
     * @return a new instance of this with the given function
     */
    public ArithmeticPostAggregation withFn(ArithmeticPostAggregationFunction fn) {
        return new ArithmeticPostAggregation(getName(), fn, getPostAggregations());
    }

    @Override
    public ArithmeticPostAggregation withPostAggregations(List<? extends PostAggregation> fields) {
        return new ArithmeticPostAggregation(getName(), getFn(), new ArrayList<>(fields));
    }

    @Override
    public List<PostAggregation> getPostAggregations() {
        return new ArrayList<>(fields);
    }

    @Override
    public String toString() {
        return "ArithmeticPostAggregation{" +
                "name=" + getName() +
                ", fn=" + getFn() +
                ", fields=" + getPostAggregations() +
                '}';
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
    }
}
