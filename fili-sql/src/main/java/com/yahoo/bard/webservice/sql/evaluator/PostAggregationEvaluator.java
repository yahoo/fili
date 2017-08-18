// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator;

import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;

import java.util.function.Function;

/**
 * Evaluates post aggregations.
 * For more info see: io.druid.query.aggregation.post.ArithmeticPostAggregator.
 *
 * To use this call {@link #calculate(PostAggregation, Function)}.
 */
public class PostAggregationEvaluator implements ReflectiveVisitor {
    private final ReflectUtil.MethodDispatcher<Double> dispatcher;

    /**
     * Constructor.
     */
    public PostAggregationEvaluator() {
        /*
        The method dispatcher dynamically calls the correct method in this class based on the polymorphic first
        argument. All methods must have the same signature except for the first argument.
         */
        dispatcher = ReflectUtil.createMethodDispatcher(
                Double.class,
                this,
                "evaluate",
                PostAggregation.class,
                Function.class
        );
    }

    /**
     * Calculates the value of a post aggregation.
     *
     * @param postAggregation  The post aggregation to evaluate.
     * @param aggregatedValues  A map from fieldNames of aggregated values to their actual value.
     *
     * @return the number calculated from the postAggregation.
     *
     * @throws UnsupportedOperationException for PostAggregations which couldn't be processed.
     */
    public Number calculate(PostAggregation postAggregation, Function<String, String> aggregatedValues) {
        Double doubleValue = dispatcher.invoke(postAggregation, aggregatedValues);
        if (postAggregation.isFloatingPoint()) {
            return doubleValue;
        } else {
            return doubleValue.longValue();
        }
    }

    /**
     * Top level evaluation of a postAggregation which evaluates all inner {@link PostAggregation}
     * and returns the value.
     *
     * @param postAggregation  The post aggregation to evaluate.
     * @param aggregatedValues  A map from fieldNames of aggregated values to their actual value.
     *
     * @return only throws exception.
     *
     * @throws UnsupportedOperationException for PostAggregations which couldn't be processed.
     */
    public Double evaluate(PostAggregation postAggregation, Function<String, String> aggregatedValues) {
        throw new UnsupportedOperationException("can't process " + postAggregation);
    }

    /**
     * Evaluates a fieldAccessorPostAggregation by parsing the value from the aggregatedValues map.
     *
     * @param fieldAccessorPostAggregation  Determines which fields value will be accessed. The field must be in the
     * `aggregatedValues` which will parse the value returned as a double.
     * @param aggregatedValues  A map from fieldNames of aggregated values to their actual value.
     *
     * @return the number parsed from the field.
     */
    public Double evaluate(
            FieldAccessorPostAggregation fieldAccessorPostAggregation,
            Function<String, String> aggregatedValues
    ) {
        String stringNumber = aggregatedValues.apply(fieldAccessorPostAggregation.getFieldName());
        return Double.valueOf(stringNumber);
    }

    /**
     * Evaluates an arithmeticPostAggregation by performing it's operation over other postAggregations.
     *
     * @param arithmeticPostAggregation  The post aggregation which performs an operation over other post aggregations.
     * @param aggregatedValues  A map from fieldNames of aggregated values to their actual value.
     *
     * @return the number calculated from it's operation.
     */
    public Double evaluate(
            ArithmeticPostAggregation arithmeticPostAggregation,
            Function<String, String> aggregatedValues
    ) {
        // todo replace switch with a map
        switch (arithmeticPostAggregation.getFn()) {
            case PLUS:
                Double sum = 0D;
                for (PostAggregation postAgg : arithmeticPostAggregation.getFields()) {
                    sum += dispatcher.invoke(postAgg, aggregatedValues);
                }
                return sum;
            case MULTIPLY:
                Double prod = 1D;
                for (PostAggregation postAgg : arithmeticPostAggregation.getFields()) {
                    prod *= dispatcher.invoke(postAgg, aggregatedValues);
                }
                return prod;
            case MINUS:
                Double sub = dispatcher.invoke(arithmeticPostAggregation.getFields().get(0), aggregatedValues);
                for (int i = 1; i < arithmeticPostAggregation.getFields().size(); i++) {
                    PostAggregation postAgg = arithmeticPostAggregation.getFields().get(i);
                    sub -= dispatcher.invoke(postAgg, aggregatedValues);
                }
                return sub;
            case DIVIDE:
                Double div = dispatcher.invoke(arithmeticPostAggregation.getFields().get(0), aggregatedValues);
                for (int i = 1; i < arithmeticPostAggregation.getFields().size(); i++) {
                    PostAggregation postAgg = arithmeticPostAggregation.getFields().get(i);
                    Double result = dispatcher.invoke(postAgg, aggregatedValues);
                    // if divisor is zero then result is zero
                    // from druid docs http://druid.io/docs/latest/querying/post-aggregations.html
                    if (result == 0.0D) {
                        return 0.0D;
                    }
                    div /= result;
                }
                return div;
        }
        throw new UnsupportedOperationException("Can't do post aggregation " + arithmeticPostAggregation);
    }

    /**
     * Evaluates a constantPostAggregation by reading it's value.
     *
     * @param constantPostAggregation  Contains a constant which will be read.
     * @param aggregatedValues  A map from fieldNames of aggregated values to their actual value.
     *
     * @return the constant value for this postAggregation.
     */
    public Double evaluate(ConstantPostAggregation constantPostAggregation, Function<String, String> aggregatedValues) {
        return constantPostAggregation.getValue();
    }
}
