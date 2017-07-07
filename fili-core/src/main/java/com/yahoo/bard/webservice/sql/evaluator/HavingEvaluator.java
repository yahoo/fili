// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator;

import com.yahoo.bard.webservice.druid.model.having.AndHaving;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.having.MultiClauseHaving;
import com.yahoo.bard.webservice.druid.model.having.NotHaving;
import com.yahoo.bard.webservice.druid.model.having.NumericHaving;
import com.yahoo.bard.webservice.druid.model.having.OrHaving;
import com.yahoo.bard.webservice.sql.ApiToFieldMapper;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Evaluates Having filters on aggregated metrics to
 * build an equivalent {@link RexNode} for making a sql query.
 */
public class HavingEvaluator {
    /**
     * Private constructor - all methods static.
     */
    private HavingEvaluator() {

    }

    /**
     * Creates an {@link Optional} which contains the given {@link Having}
     * as a {@link RexNode} or an empty value.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param having  The having filter being evaluated.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return the equivalent {@link RexNode} to be used in a sql query.
     */
    public static Optional<RexNode> buildFilter(
            RelBuilder builder,
            Having having,
            ApiToFieldMapper apiToFieldMapper
    ) {
        return Optional.ofNullable(evaluate(builder, having, apiToFieldMapper));
    }

    /**
     * Top level evaluate function which will call the correct "evaluate" method
     * based on the having type.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param having  The having filter being evaluated.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return the equivalent {@link RexNode} to be used in a sql query.
     */
    private static RexNode evaluate(RelBuilder builder, Having having, ApiToFieldMapper apiToFieldMapper) {
        if (having == null) {
            return null;
        }
        return null;
    }

    /**
     * Evaluates a {@link NumericHaving}.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param having  The NumericHaving filter to be evaluated.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return the equivalent {@link RexNode} to be used in a sql query.
     */
    private static RexNode evaluate(
            RelBuilder builder,
            NumericHaving having,
            ApiToFieldMapper apiToFieldMapper
    ) {
        Having.DefaultHavingType havingType = (Having.DefaultHavingType) having.getType();
        SqlOperator operator = null;
        switch (havingType) {
            case EQUAL_TO:
                operator = SqlStdOperatorTable.EQUALS;
                break;
            case LESS_THAN:
                operator = SqlStdOperatorTable.LESS_THAN;
                break;
            case GREATER_THAN:
                operator = SqlStdOperatorTable.GREATER_THAN;
                break;
        }
        return builder.call(
                operator,
                builder.field(apiToFieldMapper.apply(having.getAggregation())),
                builder.literal(having.getValue())
        );
    }

    /**
     * Evaluates a {@link NotHaving} filter.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param notHaving  The not having filter to be converted to be evaluated.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return the equivalent {@link RexNode} to be used in a sql query.
     */
    private static RexNode evaluate(RelBuilder builder, NotHaving notHaving, ApiToFieldMapper apiToFieldMapper) {
        return builder.call(
                SqlStdOperatorTable.NOT,
                evaluate(builder, notHaving.getHaving(), apiToFieldMapper)
        );
    }

    /**
     * Evaluates a {@link OrHaving}.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param orHaving  The OrHaving to be evaluated.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return the equivalent {@link RexNode} to be used which ORs over the inner havings.
     */
    private static RexNode evaluate(RelBuilder builder, OrHaving orHaving, ApiToFieldMapper apiToFieldMapper) {
        return listEvaluate(builder, orHaving, SqlStdOperatorTable.OR, apiToFieldMapper);
    }

    /**
     * Evaluates a {@link AndHaving}.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param andHaving  The AndHaving to be evaluated.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return the equivalent {@link RexNode} to be used which ANDs over the inner havings.
     */
    private static RexNode evaluate(RelBuilder builder, AndHaving andHaving, ApiToFieldMapper apiToFieldMapper) {
        return listEvaluate(builder, andHaving, SqlStdOperatorTable.AND, apiToFieldMapper);
    }

    /**
     * Evaluates a {@link MultiClauseHaving} filter by performing it's operation over a list of other havings.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param multiClauseHaving  The MultiClauseHaving filter to be evaluated.
     * @param operator  The operator to be performed over the inner clauses of this having filter.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return the equivalent {@link RexNode} to be used in a sql query.
     */
    private static RexNode listEvaluate(
            RelBuilder builder,
            MultiClauseHaving multiClauseHaving,
            SqlOperator operator,
            ApiToFieldMapper apiToFieldMapper
    ) {
        List<RexNode> rexNodes = multiClauseHaving.getHavings()
                .stream()
                .map(having -> evaluate(builder, having, apiToFieldMapper))
                .collect(Collectors.toList());

        return builder.call(
                operator,
                rexNodes
        );
    }
}
