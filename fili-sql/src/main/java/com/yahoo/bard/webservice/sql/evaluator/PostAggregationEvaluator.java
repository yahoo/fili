// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator;

import com.google.common.collect.ImmutableList;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.sql.ApiToFieldMapper;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Translates recursive postAggregations to sql arithmetic.
 *
 * To use this call {@link #evaluatePostAggregation(PostAggregation, RelBuilder, ApiToFieldMapper)}
 * */
public class PostAggregationEvaluator implements ReflectiveVisitor {
    private final ReflectUtil.MethodDispatcher<RexNode> dispatcher;

    private final static String MULTI_OP_CALL_ERROR_MSG =
            "It takes two or more fields to do a add operation, but only got %d";
    /**
     * Constructor.
     * */
    public PostAggregationEvaluator() {
        /*
         * The method dispatcher dynamically calls the correct method in this class based on the polymorphic first
         * argument. All methods must have the same signature except for the first argument.
         */
        dispatcher = ReflectUtil.createMethodDispatcher(
                RexNode.class,
                this,
                "evaluate",
                PostAggregation.class,
                RelBuilder.class,
                ApiToFieldMapper.class
        );
    }

    /**
     * Transforms sql arithmetic from post aggregation by invoking dispatcher.
     * This method is directly called for using this class.
     *
     * @param postAggregation  The post aggregation to evaluate.
     * @param builder  The RelBuilder for building sql query.
     * @param apiToFieldMapper Maps between logical and physical column names given a table schema
     *
     * @return the RexNode of sql arithmetic
     *
     * @throws UnsupportedOperationException for PostAggregations which couldn't be processed.
     */
    public RexNode evaluatePostAggregation(
            PostAggregation postAggregation,
            RelBuilder builder,
            ApiToFieldMapper apiToFieldMapper) {
        if (postAggregation == null) {
            return null;
        }
        return dispatcher.invoke(postAggregation, builder, apiToFieldMapper);
    }

    /**
     * Top level evaluation of a postAggregation which evaluates all inner {@link PostAggregation}
     * and returns the value.
     *
     * @param postAggregation  The post aggregation to evaluate.
     * @param builder  The RelBuilder for building sql query.
     * @param apiToFieldMapper Maps between logical and physical column names given a table schema
     *
     * @return the RexNode of sql arithmetic
     *
     * @throws UnsupportedOperationException for PostAggregations which couldn't be processed.
     */
    public RexNode evaluate(PostAggregation postAggregation, RelBuilder builder, ApiToFieldMapper apiToFieldMapper) {
        throw new UnsupportedOperationException("Can't Process " + postAggregation);
    }

    /**
     * Evaluates a fieldAccessorPostAggregation by parsing the value from the aggregatedValues map.
     * @param fieldAccessorPostAggregation  The post aggregation to evaluate.
     * @param builder  The RelBuilder for building sql query.
     * @param apiToFieldMapper Maps between logical and physical column names given a table schema
     *
     * @return the RexNode of sql arithmetic
     *
     * @throws UnsupportedOperationException for PostAggregations which couldn't be processed.
     */
    public RexNode evaluate(
            FieldAccessorPostAggregation fieldAccessorPostAggregation,
            RelBuilder builder,
            ApiToFieldMapper apiToFieldMapper
    ) {
        return builder.field(apiToFieldMapper.unApply(fieldAccessorPostAggregation.getFieldName()));
    }

    /**
     * Evaluates a constantPostAggregation by reading it's value.
     *
     * @param constantPostAggregation  The post aggregation to evaluate.
     * @param builder  The RelBuilder for building sql query.
     * @param apiToFieldMapper Maps between logical and physical column names given a table schema
     *
     * @return the RexNode of sql arithmetic
     *
     * @throws UnsupportedOperationException for PostAggregations which couldn't be processed.
     */
    public RexNode evaluate(
            ConstantPostAggregation constantPostAggregation,
            RelBuilder builder,
            ApiToFieldMapper apiToFieldMapper
    ) {
        return builder.literal(constantPostAggregation.getValue());
    }

    /**
     * Build the add call RexNode.
     * @param addFields all the fields to be added together
     * @param builder The RelBuilder for building sql query.
     * @param operator The operator of the call
     * @return The RexNode represents the add operation
     */
    private RexNode buildMultiOpCall(List<RexNode> addFields, RelBuilder builder, SqlBinaryOperator operator) {
        if (addFields.size() < 2) {
            throw new IllegalStateException(String.format(MULTI_OP_CALL_ERROR_MSG, addFields.size()));
        }
        RexNode previousAdd = builder.call(
                operator,
                ImmutableList.of(addFields.get(0),  addFields.get(1))
        );
        int i = 2;
        while (i < addFields.size()) {
            previousAdd = builder.call(operator, ImmutableList.of(previousAdd,  addFields.get(i)));
            i++;
        }
        return previousAdd;
    }
    /**
     * Evaluates an arithmeticPostAggregation by translating it's operation over other postAggregations
     * to sql arithmetic.
     *
     * @param arithmeticPostAggregation  The post aggregation to evaluate.
     * @param builder  The RelBuilder for building sql query.
     * @param apiToFieldMapper Maps between logical and physical column names given a table schema
     *
     * @return the RexNode of sql arithmetic
     *
     * @throws UnsupportedOperationException for PostAggregations which couldn't be processed.
     * @throws IllegalStateException for when buildMultiOpCall doesn't receive correct number of ops
     */
    public RexNode evaluate(
            ArithmeticPostAggregation arithmeticPostAggregation,
            RelBuilder builder,
            ApiToFieldMapper apiToFieldMapper) {
        List<RexNode> innerFields = arithmeticPostAggregation.getPostAggregations().stream()
                .map(field -> dispatcher.invoke(field, builder, apiToFieldMapper))
                .collect(Collectors.toList());

        switch (arithmeticPostAggregation.getFn()) {
            case PLUS:
                return builder.alias(
                        buildMultiOpCall(innerFields, builder, SqlStdOperatorTable.PLUS),
                        arithmeticPostAggregation.getName()
                );
            case MULTIPLY:
                return builder.alias(
                        buildMultiOpCall(innerFields, builder, SqlStdOperatorTable.MULTIPLY),
                        arithmeticPostAggregation.getName()
                );
            case MINUS:
                assert innerFields.size() == 2;
                return builder.alias(
                        builder.call(SqlStdOperatorTable.MINUS, innerFields),
                        arithmeticPostAggregation.getName()
                );
            case DIVIDE:
                assert innerFields.size() == 2;
                List<RexNode> temp = new ArrayList<>();
                //cast Integer to Double to avoid truncation
                RexNode numerator =
                        builder.call(SqlStdOperatorTable.MULTIPLY, builder.literal(1.0), innerFields.get(0));
                RexNode denominator = innerFields.get(1);
                if (denominator.getKind() == SqlKind.AS) {
                    denominator = ((RexCall) denominator).operands.get(0);
                }
                temp.add(numerator);
                temp.add(denominator);
                return builder.alias(
                        builder.call(SqlStdOperatorTable.DIVIDE, temp),
                        arithmeticPostAggregation.getName()
                );
        }
        throw new UnsupportedOperationException("Can't do post aggregation " + arithmeticPostAggregation);
    }
}
