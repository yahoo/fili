// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType;

import java.util.function.Function;

/**
 * Evaluates post aggregations.
 */
public class PostAggregationEvaluator {

    /**
     * Private constructor - all methods static.
     */
    private PostAggregationEvaluator() {

    }

    /**
     * Top level evaluation of a postAggregation.
     * NOTE: does not support any sketch operations.
     * TODO: always returns a double. this may not be correct
     *
     * @param postAggregation  The post aggregation to evaluate.
     * @param aggregatedValues  A map from fieldNames of aggregated values to their actual value.
     *
     * @return the number calculated from the postAggregation.
     */
    public static Double evaluate(PostAggregation postAggregation, Function<String, String> aggregatedValues) {
        DefaultPostAggregationType aggregationType = (DefaultPostAggregationType) postAggregation
                .getType();

        switch (aggregationType) {
            case ARITHMETIC:
                ArithmeticPostAggregation arithmeticPostAggregation = (ArithmeticPostAggregation) postAggregation;
                return evaluate(arithmeticPostAggregation, aggregatedValues);
            case FIELD_ACCESS:
                FieldAccessorPostAggregation fieldAccessorPostAggregation = (FieldAccessorPostAggregation)
                        postAggregation;
                return evaluate(fieldAccessorPostAggregation, aggregatedValues);
            case CONSTANT:
                ConstantPostAggregation constantPostAggregation = (ConstantPostAggregation) postAggregation;
                return evaluate(constantPostAggregation);
            case THETA_SKETCH_ESTIMATE:
            case THETA_SKETCH_SET_OP:
            case SKETCH_ESTIMATE:
            case SKETCH_SET_OPER:
            default:
                throw new UnsupportedOperationException("Can't do post aggregation " + postAggregation.getType());
        }
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
                if (arithmeticPostAggregation.getFields().size() != 2) { // todo check if this is true
                    throw new IllegalArgumentException("Can only subtract on two fields");
                }
                Double firstAsDoubleSub = evaluate(arithmeticPostAggregation.getFields().get(0), aggregatedValues);
                Double secondAsDoubleSub = evaluate(arithmeticPostAggregation.getFields().get(1), aggregatedValues);
                return firstAsDoubleSub - secondAsDoubleSub;
            case DIVIDE:
                if (arithmeticPostAggregation.getFields().size() != 2) {
                    throw new IllegalArgumentException("Can only divide on two fields");
                }
                Double firstAsDoubleDiv = evaluate(arithmeticPostAggregation.getFields().get(0), aggregatedValues);
                Double secondAsDoubleDiv = evaluate(arithmeticPostAggregation.getFields().get(1), aggregatedValues);
                if (secondAsDoubleDiv == 0) {
                    // if divisor is zero then result is zero
                    // as per druid's http://druid.io/docs/latest/querying/post-aggregations.html
                    return 0D;
                }
                return firstAsDoubleDiv / secondAsDoubleDiv;
        }
        throw new UnsupportedOperationException("Can't do post aggregation " + arithmeticPostAggregation);
    }

    /**
     * Evaluates a constantPostAggregation by reading it's value.
     *
     * @param constantPostAggregation  Contains a constant which will be read.
     *
     * @return the constant value for this postAggregation.
     */
    private static double evaluate(ConstantPostAggregation constantPostAggregation) {
        return constantPostAggregation.getValue();
    }
}
