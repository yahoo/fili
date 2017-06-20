// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.having.MultiClauseHaving;
import com.yahoo.bard.webservice.druid.model.having.NotHaving;
import com.yahoo.bard.webservice.druid.model.having.NumericHaving;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
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

    public static Optional<RexNode> buildFilter(
            RelBuilder builder,
            Having having,
            Function<String, String> aliasMaker
    ) {
        return Optional.ofNullable(evaluate(builder, having, aliasMaker));
    }

    /**
     * Creates a having filter which will filter aggregated dimensions.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param having  The having filter being evaluated.
     * @param aliasMaker  A function to get the aliased aggregation's name from the metric name.
     *
     * @return the equivalent RexNode to be used in a sql query.
     */
    private static RexNode evaluate(RelBuilder builder, Having having, Function<String, String> aliasMaker) {
        if (having == null) {
            return null;
        }
        Having.DefaultHavingType havingType = (Having.DefaultHavingType) having.getType();

        switch (havingType) {
            case AND:
                return listEvaluate(builder, (MultiClauseHaving) having, SqlStdOperatorTable.AND, aliasMaker);
            case OR:
                return listEvaluate(builder, (MultiClauseHaving) having, SqlStdOperatorTable.OR, aliasMaker);
            case NOT:
                NotHaving notHaving = (NotHaving) having;
                return evaluate(builder, notHaving, aliasMaker);
            case EQUAL_TO:
                return numericEvaluate(builder, (NumericHaving) having, SqlStdOperatorTable.EQUALS, aliasMaker);
            case LESS_THAN:
                return numericEvaluate(builder, (NumericHaving) having, SqlStdOperatorTable.LESS_THAN, aliasMaker);
            case GREATER_THAN:
                return numericEvaluate(builder, (NumericHaving) having, SqlStdOperatorTable.GREATER_THAN, aliasMaker);
        }

        throw new UnsupportedOperationException("Can't evaluate having " + having);
    }

    /**
     * Evaluates a NotHaving filter.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param notHaving  The not having filter to be converted to be evaluated.
     * @param aliasMaker  A function to get the aliased aggregation's name from the metric name.
     *
     * @return the equivalent RexNode to be used in a sql query.
     */
    private static RexNode evaluate(RelBuilder builder, NotHaving notHaving, Function<String, String> aliasMaker) {
        return builder.call(
                SqlStdOperatorTable.NOT,
                evaluate(builder, notHaving.getHaving(), aliasMaker)
        );
    }

    /**
     * Evaluates a numericHaving .
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param having  The NumericHaving filter to be evaluated.
     * @param operator  The operator to be performed between the field and value.
     * @param aliasMaker  A function to get the aliased aggregation's name from the metric name.
     *
     * @return the equivalent RexNode to be used in a sql query.
     */
    private static RexNode numericEvaluate(
            RelBuilder builder,
            NumericHaving having,
            SqlBinaryOperator operator,
            Function<String, String> aliasMaker
    ) {
        return builder.call(
                operator,
                builder.field(aliasMaker.apply(having.getAggregation())),
                builder.literal(having.getValue())
        );

    }

    /**
     * Evaluates a MultiClauseHaving filter by performing it's operation over a list of other havings.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param multiClauseHaving  The MultiClauseHaving filter to be evaluated.
     * @param operator  The operator to be performed over the inner clauses of this having filter.
     * @param aliasMaker  A function to get the aliased aggregation's name from the metric name.
     *
     * @return the equivalent RexNode to be used in a sql query.
     */
    private static RexNode listEvaluate(
            RelBuilder builder,
            MultiClauseHaving multiClauseHaving,
            SqlOperator operator,
            Function<String, String> aliasMaker
    ) {
        List<RexNode> rexNodes = multiClauseHaving.getHavings()
                .stream()
                .map(having -> evaluate(builder, having, aliasMaker))
                .collect(Collectors.toList());

        return builder.call(
                operator,
                rexNodes
        );
    }
}
