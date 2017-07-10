// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator;

import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchEstimatePostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchEstimatePostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchSetOperationPostAggregation;

import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;

import java.util.function.Function;

/**
 * Evaluates post aggregations.
 * For more info see: io.druid.query.aggregation.post.ArithmeticPostAggregator.
 */
public class PostAggregationEvaluator implements ReflectiveVisitor {
    private final ReflectUtil.MethodDispatcher<Double> dispatcher;
    private Function<String, String> aggregatedValues; //todo maybe not needed

    /**
     * Constructor
     *
     */
    public PostAggregationEvaluator() {
        dispatcher = ReflectUtil.createMethodDispatcher(
                Double.class,
                this,
                "evaluate",
                PostAggregation.class
        );
    }

    /**
     * Calculates the value of a post aggregation.
     *
     * @param postAggregation  The post aggregation to evaluate.
     * @param aggregatedValues  A map from fieldNames of aggregated values to their actual value.
     *
     * @return the number calculated from the postAggregation.
     */
    public Number calculate(PostAggregation postAggregation, Function<String, String> aggregatedValues) {
        this.aggregatedValues = aggregatedValues;
        Double doubleValue = dispatcher.invoke(postAggregation);
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
     *
     * @return the number calculated from the postAggregation.
     */
    public Double evaluate(PostAggregation postAggregation) {
        throw new UnsupportedOperationException("can't process " + postAggregation);
    }

    /**
     * Evaluates a fieldAccessorPostAggregation by parsing the value from the aggregatedValues map.
     *
     * @param fieldAccessorPostAggregation  Determines which fields value will be accessed. The field must be in the
     * map.
     *
     * @return the number parsed from the field.
     */
    public Double evaluate(FieldAccessorPostAggregation fieldAccessorPostAggregation) {
        String stringNumber = aggregatedValues.apply(fieldAccessorPostAggregation.getFieldName());
        return Double.valueOf(stringNumber);
    }

    /**
     * Evaluates an arithmeticPostAggregation by performing it's operation over other postAggregations.
     *
     * @param arithmeticPostAggregation  The post aggregation which performs an operation over other post aggregations.
     *
     * @return the number calculated from it's operation.
     */
    public Double evaluate(ArithmeticPostAggregation arithmeticPostAggregation) {
        switch (arithmeticPostAggregation.getFn()) {
            case PLUS:
                Double sum = 0D;
                for (PostAggregation postAgg : arithmeticPostAggregation.getFields()) {
                    sum += dispatcher.invoke(postAgg);
                }
                return sum;
            case MULTIPLY:
                Double prod = 1D;
                for (PostAggregation postAgg : arithmeticPostAggregation.getFields()) {
                    prod *= dispatcher.invoke(postAgg);
                }
                return prod;
            case MINUS:
                Double sub = dispatcher.invoke(arithmeticPostAggregation.getFields().get(0));
                for (int i = 1; i < arithmeticPostAggregation.getFields().size(); i++) {
                    PostAggregation postAgg = arithmeticPostAggregation.getFields().get(i);
                    sub -= dispatcher.invoke(postAgg);
                }
                return sub;
            case DIVIDE:
                if (arithmeticPostAggregation.getFields().size() != 2) {
                    throw new IllegalArgumentException("Can only divide on two fields");
                }
                Double divLhs = dispatcher.invoke(arithmeticPostAggregation.getFields().get(0));
                Double divRhs = dispatcher.invoke(arithmeticPostAggregation.getFields().get(1));
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
     *
     * @return the constant value for this postAggregation.
     */
    public Double evaluate(ConstantPostAggregation constantPostAggregation) {
        return constantPostAggregation.getValue();
    }
}
