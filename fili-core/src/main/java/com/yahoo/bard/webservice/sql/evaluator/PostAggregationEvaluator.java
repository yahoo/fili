// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator;

import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import java.util.function.Function;

/**
 * Evaluates post aggregations.
 * For more info see: io.druid.query.aggregation.post.ArithmeticPostAggregator.
 */
public class PostAggregationEvaluator {

    /**
     * Private constructor - all methods static.
     */
    private PostAggregationEvaluator() {

    }

    /**
     * Calculates the value of a post aggregation.
     *
     * @param postAggregation  The post aggregation to evaluate.
     * @param aggregatedValues  A map from fieldNames of aggregated values to their actual value.
     *
     * @return the number calculated from the postAggregation.
     */
    public static Number calculate(PostAggregation postAggregation, Function<String, String> aggregatedValues) {
        Double doubleValue = evaluate(postAggregation, aggregatedValues);
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
     * @return the number calculated from the postAggregation.
     */
    private static Double evaluate(PostAggregation postAggregation, Function<String, String> aggregatedValues) {
        if (postAggregation == null) {
            return null;
        }
        return null;
    }

    /**
     * Evaluates a fieldAccessorPostAggregation by parsing the value from the aggregatedValues map.
     *
     * @param fieldAccessorPostAggregation  Determines which fields value will be accessed. The field must be in the
     * map.
     * @param aggregatedValues  A map from fieldNames of aggregated values to their actual value.
     *
     * @return the number parsed from the field.
     */
    private static Double evaluate(
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
    private static Double evaluate(
            ArithmeticPostAggregation arithmeticPostAggregation,
            Function<String, String> aggregatedValues
    ) {
        switch (arithmeticPostAggregation.getFn()) {
            case PLUS:
                Double sum = 0D;
                for (PostAggregation postAgg : arithmeticPostAggregation.getFields()) {
                    sum += evaluate(postAgg, aggregatedValues);
                }
                return sum;
            case MULTIPLY:
                Double prod = 1D;
                for (PostAggregation postAgg : arithmeticPostAggregation.getFields()) {
                    prod *= evaluate(postAgg, aggregatedValues);
                }
                return prod;
            case MINUS:
                Double sub = evaluate(arithmeticPostAggregation.getFields().get(0), aggregatedValues);
                for (int i = 1; i < arithmeticPostAggregation.getFields().size(); i++) {
                    PostAggregation postAgg = arithmeticPostAggregation.getFields().get(i);
                    sub -= evaluate(postAgg, aggregatedValues);
                }
                return sub;
            case DIVIDE:
                if (arithmeticPostAggregation.getFields().size() != 2) {
                    throw new IllegalArgumentException("Can only divide on two fields");
                }
                Double divLhs = evaluate(arithmeticPostAggregation.getFields().get(0), aggregatedValues);
                Double divRhs = evaluate(arithmeticPostAggregation.getFields().get(1), aggregatedValues);
                // if divisor is zero then result is zero
                // from druid docs http://druid.io/docs/latest/querying/post-aggregations.html
                return divRhs == 0.0 ? 0 : divLhs / divRhs;
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
    private static double evaluate(
            ConstantPostAggregation constantPostAggregation,
            Function<String, String> aggregatedValues
    ) {
        return constantPostAggregation.getValue();
    }
}
