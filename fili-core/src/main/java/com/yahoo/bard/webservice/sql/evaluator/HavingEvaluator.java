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
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Evaluates Having filters on aggregated metrics to
 * build an equivalent {@link RexNode} for making a sql query.
 */
public class HavingEvaluator implements ReflectiveVisitor {
    private RelBuilder builder;
    private final ReflectUtil.MethodDispatcher<RexNode> dispatcher;
    private ApiToFieldMapper apiToFieldMapper; //todo maybe not needed

    /**
     * Constructor.
     */
    public HavingEvaluator() {
        dispatcher = ReflectUtil.createMethodDispatcher(
                RexNode.class,
                this,
                "evaluate",
                Having.class
        );
    }

    /**
     * Creates an {@link RexNode} which contains the given {@link Having}.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param having  The having filter being evaluated.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return the equivalent {@link RexNode} to be used in a sql query.
     *
     * @throws UnsupportedOperationException for havings which couldn't be evaluated.
     */
    public RexNode evaluateHaving(
            RelBuilder builder,
            Having having,
            ApiToFieldMapper apiToFieldMapper
    ) {
        this.builder = builder;
        this.apiToFieldMapper = apiToFieldMapper;
        return dispatcher.invoke(having);
    }

    /**
     * Top level evaluate function meant to capture {@link Having} which could not be mapped
     * to a specific "evaluate" method.
     *
     * @param having  The having filter being evaluated.
     *
     * @return only throws exception.
     *
     * @throws UnsupportedOperationException for havings which couldn't be evaluated.
     */
    public RexNode evaluate(Having having) {
        throw new UnsupportedOperationException("Can't Process " + having);
    }

    /**
     * Evaluates a {@link NumericHaving}.
     *
     * @param having  The NumericHaving filter to be evaluated.
     *
     * @return the equivalent {@link RexNode} to be used in a sql query.
     */
    public RexNode evaluate(NumericHaving having) {
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
     * @param notHaving  The not having filter to be converted to be evaluated.
     *
     * @return the equivalent {@link RexNode} to be used in a sql query.
     */
    public RexNode evaluate(NotHaving notHaving) {
        return builder.call(
                SqlStdOperatorTable.NOT,
                dispatcher.invoke(notHaving.getHaving())
        );
    }

    /**
     * Evaluates a {@link OrHaving}.
     *
     * @param orHaving  The OrHaving to be evaluated.
     *
     * @return the equivalent {@link RexNode} to be used which ORs over the inner havings.
     */
    public RexNode evaluate(OrHaving orHaving) {
        return listEvaluate(orHaving, SqlStdOperatorTable.OR);
    }

    /**
     * Evaluates a {@link AndHaving}.
     *
     * @param andHaving  The AndHaving to be evaluated.
     *
     * @return the equivalent {@link RexNode} to be used which ANDs over the inner havings.
     */
    public RexNode evaluate(AndHaving andHaving) {
        return listEvaluate(andHaving, SqlStdOperatorTable.AND);
    }

    /**
     * Evaluates a {@link MultiClauseHaving} filter by performing it's operation over a list of other havings.
     *
     * @param multiClauseHaving  The MultiClauseHaving filter to be evaluated.
     * @param operator  The operator to be performed over the inner clauses of this having filter.
     *
     * @return the equivalent {@link RexNode} to be used in a sql query.
     */
    public RexNode listEvaluate(
            MultiClauseHaving multiClauseHaving,
            SqlOperator operator
    ) {
        List<RexNode> rexNodes = multiClauseHaving.getHavings()
                .stream()
                .map(dispatcher::invoke)
                .collect(Collectors.toList());

        return builder.call(
                operator,
                rexNodes
        );
    }
}
