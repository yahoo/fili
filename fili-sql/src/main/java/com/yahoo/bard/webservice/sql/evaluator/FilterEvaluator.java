// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.filter.AndFilter;
import com.yahoo.bard.webservice.druid.model.filter.ComplexFilter;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.filter.InFilter;
import com.yahoo.bard.webservice.druid.model.filter.NotFilter;
import com.yahoo.bard.webservice.druid.model.filter.OrFilter;
import com.yahoo.bard.webservice.druid.model.filter.RegularExpressionFilter;
import com.yahoo.bard.webservice.druid.model.filter.SearchFilter;
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter;
import com.yahoo.bard.webservice.sql.ApiToFieldMapper;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Evaluates filters to find all the dimensions used in them and
 * to build a {@link RexNode} with an equivalent sql filter. To use this call
 * {@link #evaluateFilter(Filter, RelBuilder, ApiToFieldMapper)}.
 */
public class FilterEvaluator implements ReflectiveVisitor {
    private final ReflectUtil.MethodDispatcher<RexNode> dispatcher;

    /**
     * Constructor.
     */
    public FilterEvaluator() {
        /*
        The method dispatcher dynamically calls the correct method in this class based on the polymorphic first
        argument. All methods must have the same signature except for the first argument.
         */
        dispatcher = ReflectUtil.createMethodDispatcher(
                RexNode.class,
                this,
                "evaluate",
                Filter.class,
                RelBuilder.class,
                ApiToFieldMapper.class
        );
    }

    /**
     * Evaluates and builds a filter and finds all the dimension names used in all filters.
     *
     * @param filter  The filter to be evaluated.
     * @param builder  The RelBuilder used to build queries with Calcite.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return a RexNode containing an equivalent filter to the one given.
     *
     * @throws UnsupportedOperationException for filters which couldn't be evaluated.
     */
    public RexNode evaluateFilter(Filter filter, RelBuilder builder, ApiToFieldMapper apiToFieldMapper) {
        if (filter == null) {
            return null;
        }

        return dispatcher.invoke(filter, builder, apiToFieldMapper);
    }

    /**
     * Top level evaluate call meant to capture {@link Filter} which could not be mapped
     * to a specific "evaluate" method.
     *
     * @param filter  The filter to be evaluated.
     * @param builder  The RelBuilder used to build queries with Calcite.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return only throws exception.
     *
     * @throws UnsupportedOperationException for filters which couldn't be evaluated.
     */
    public RexNode evaluate(Filter filter, RelBuilder builder, ApiToFieldMapper apiToFieldMapper) {
        throw new UnsupportedOperationException("Can't Process " + filter);
    }

    /**
     * Evaluates a regular expression filter.
     *
     * @param regexFilter  A regexFilter to be evaluated.
     * @param builder  The RelBuilder used to build queries with Calcite.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return a RexNode containing an equivalent filter to the one given.
     */
    public RexNode evaluate(
            RegularExpressionFilter regexFilter,
            RelBuilder builder,
            ApiToFieldMapper apiToFieldMapper
    ) {
        // todo test this
        String apiName = regexFilter.getDimension().getApiName();
        return builder.call(
                SqlStdOperatorTable.LIKE,
                builder.field(apiToFieldMapper.apply(apiName)),
                builder.literal(regexFilter.getPattern().toString())
        );
    }

    /**
     * Evaluates a Selector filter.
     *
     * @param selectorFilter  A selectorFilter to be evaluated.
     * @param builder  The RelBuilder used to build queries with Calcite.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return a RexNode containing an equivalent filter to the one given.
     */
    public RexNode evaluate(SelectorFilter selectorFilter, RelBuilder builder, ApiToFieldMapper apiToFieldMapper) {
        String apiName = selectorFilter.getDimension().getApiName();
        return builder.call(
                SqlStdOperatorTable.EQUALS,
                builder.field(apiToFieldMapper.apply(apiName)),
                builder.literal(selectorFilter.getValue())
        );
    }

    /**
     * Evaluates a SearchFilter filter. Currently doesn't support Fragment mode.
     *
     * @param searchFilter  A searchFilter to be evaluated.
     * @param builder  The RelBuilder used to build queries with Calcite.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return a RexNode containing an equivalent filter to the one given.
     */
    public RexNode evaluate(SearchFilter searchFilter, RelBuilder builder, ApiToFieldMapper apiToFieldMapper) {
        String searchType = searchFilter.getQueryType();
        SearchFilter.QueryType optionalQuery = SearchFilter.QueryType.fromType(searchType)
                .orElseThrow(() -> new IllegalArgumentException("Couldn't convert " + searchType + " to a QueryType."));

        String columnName = searchFilter.getDimension().getApiName();
        String valueToFind = searchFilter.getQueryValue();

        switch (optionalQuery) {
            case Contains:
                return builder.call(
                        SqlStdOperatorTable.LIKE,
                        builder.field(apiToFieldMapper.apply(columnName)),
                        builder.literal("%" + valueToFind + "%")
                );
            case InsensitiveContains:
                // todo maybe look at SqlCollation
                return builder.call(
                        SqlStdOperatorTable.LIKE,
                        builder.call(
                                SqlStdOperatorTable.LOWER,
                                builder.field(apiToFieldMapper.apply(columnName))
                        ),
                        builder.literal("%" + valueToFind.toLowerCase(Locale.ENGLISH) + "%")
                );
            case Fragment:
                // todo: fragment takes json array of strings and checks if any are contained? just OR search over them?
                // http://druid.io/docs/0.9.1.1/querying/filters.html
            default:
                throw new UnsupportedOperationException(optionalQuery + " not implemented.");
        }
    }

    /**
     * Evaluates an Infilter filter.
     *
     * @param inFilter  An inFilter to be evaluated.
     * @param builder  The RelBuilder used to build queries with Calcite.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return a RexNode containing an equivalent filter to the one given.
     */
    public RexNode evaluate(InFilter inFilter, RelBuilder builder, ApiToFieldMapper apiToFieldMapper) {
        Dimension dimension = inFilter.getDimension();

        OrFilter orFilterOfSelectors = new OrFilter(
                inFilter.getValues()
                        .stream()
                        .map(value -> new SelectorFilter(dimension, value))
                        .collect(Collectors.toList())
        );
        return dispatcher.invoke(orFilterOfSelectors);
    }

    /**
     * Evaluates an {@link OrFilter}.
     *
     * @param orFilter  An orFilter to be evaluated.
     * @param builder  The RelBuilder used to build queries with Calcite.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return a RexNode containing an equivalent filter which ORs over the inner filters.
     */
    public RexNode evaluate(OrFilter orFilter, RelBuilder builder, ApiToFieldMapper apiToFieldMapper) {
        return listEvaluate(orFilter, SqlStdOperatorTable.OR, builder, apiToFieldMapper);
    }

    /**
     * Evaluates an {@link AndFilter}.
     *
     * @param andFilter  An andFilter to be evaluated.
     * @param builder  The RelBuilder used to build queries with Calcite.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return a RexNode containing an equivalent filter which ANDs over the inner filters.
     */
    public RexNode evaluate(AndFilter andFilter, RelBuilder builder, ApiToFieldMapper apiToFieldMapper) {
        return listEvaluate(andFilter, SqlStdOperatorTable.AND, builder, apiToFieldMapper);
    }

    /**
     * Evaluates an {@link NotFilter}.
     *
     * @param notFilter  An notFilter to be evaluated.
     * @param builder  The RelBuilder used to build queries with Calcite.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return a RexNode containing an equivalent filter which NOTs over the inner filters.
     */
    public RexNode evaluate(NotFilter notFilter, RelBuilder builder, ApiToFieldMapper apiToFieldMapper) {
        return listEvaluate(notFilter, SqlStdOperatorTable.NOT, builder, apiToFieldMapper);
    }

    /**
     * Evaluates a complex filter by performing a {@link SqlOperator} over a list of dimensions.
     *
     * @param complexFilter  A complexFilter to be evaluated.
     * @param operator  The sql operator to be applied to a complexFilter's fields.
     * @param builder  The RelBuilder used to build queries with Calcite.
     * @param apiToFieldMapper  A function to get the aliased aggregation's name from the metric name.
     *
     * @return a RexNode containing an equivalent filter to the one given.
     */
    private RexNode listEvaluate(
            ComplexFilter complexFilter,
            SqlOperator operator,
            RelBuilder builder,
            ApiToFieldMapper apiToFieldMapper
    ) {
        List<RexNode> rexNodes = complexFilter.getFields()
                .stream()
                .map(filter -> dispatcher.invoke(filter, builder, apiToFieldMapper))
                .collect(Collectors.toList());

        return builder.call(
                operator,
                rexNodes
        );
    }
}
