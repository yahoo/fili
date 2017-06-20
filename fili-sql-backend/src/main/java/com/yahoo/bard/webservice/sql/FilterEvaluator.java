// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.filter.ComplexFilter;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType;
import com.yahoo.bard.webservice.druid.model.filter.InFilter;
import com.yahoo.bard.webservice.druid.model.filter.OrFilter;
import com.yahoo.bard.webservice.druid.model.filter.RegularExpressionFilter;
import com.yahoo.bard.webservice.druid.model.filter.SearchFilter;
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Evaluates filters to find all the dimensions used in them and
 * to build a {@link RexNode} with an equivalent sql filter.
 */
public class FilterEvaluator {
    /**
     * Private constructor - all methods static.
     */
    private FilterEvaluator() {

    }

    /**
     * Finds all the dimension names used in the filter.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param filter  The filter to be evaluated.
     *
     * @return a list of all the dimension names.
     */
    public static List<String> getDimensionNames(RelBuilder builder, Filter filter) {
        return evaluate(builder, filter).getRight();
    }

    /**
     * Builds a {@link RexNode} with an equivalent sql filter as the one given.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param filter  The filter to be evaluated.
     *
     * @return the rexNode containing the filter information.
     */
    public static Optional<RexNode> getFilterAsRexNode(RelBuilder builder, Filter filter) {
        return evaluate(builder, filter).getLeft();
    }

    /**
     * Evaluates and builds a filter and finds all the dimension names used in all filters.
     * NOTE: this is doing two things at once, but the interface given only lets you do one at a time.
     * This is because dimension names needed to build the RexNode aren't known beforehand.
     * This forces the flow to be [getDimensionNames] -> [buider.project] -> [builer.getFilterAsRexNode]
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param filter  The filter to be evaluated.
     *
     * @return both the filter and the list of dimensions used in the filter.
     */
    private static Pair<Optional<RexNode>, List<String>> evaluate(RelBuilder builder, Filter filter) {
        List<String> dimensions = new ArrayList<>();
        Optional<RexNode> rexNode = Optional.ofNullable(evaluate(builder, filter, dimensions));
        dimensions = dimensions.stream().distinct().collect(Collectors.toList());
        return Pair.of(rexNode, dimensions);
    }

    /**
     * Top level evaluate call which finds the specialized evaluation for a given filter.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param filter  The filter to be evaluated.
     * @param dimensions  The list of dimensions already found.
     *
     * @return a RexNode containing an equivalent filter to the one given.
     */
    private static RexNode evaluate(RelBuilder builder, Filter filter, List<String> dimensions) {
        if (filter == null) {
            return null;
        }

        DefaultFilterType defaultFilterType = (DefaultFilterType) filter.getType();
        switch (defaultFilterType) {
            case SELECTOR:
                SelectorFilter selectorFilter = (SelectorFilter) filter;
                return evaluate(builder, selectorFilter, dimensions);
            case REGEX:
                RegularExpressionFilter regexFilter = (RegularExpressionFilter) filter;
                return evaluate(builder, regexFilter, dimensions);
            case AND:
                return listEvaluate(builder, (ComplexFilter) filter, dimensions, SqlStdOperatorTable.AND);
            case OR:
                return listEvaluate(builder, (ComplexFilter) filter, dimensions, SqlStdOperatorTable.OR);
            case NOT:
                return listEvaluate(builder, (ComplexFilter) filter, dimensions, SqlStdOperatorTable.NOT);
            case EXTRACTION:
                throw new UnsupportedOperationException(
                        "Not implemented. Also deprecated use selector filter with extraction function");
            case SEARCH:
                SearchFilter searchFilter = (SearchFilter) filter;
                return evaluate(builder, searchFilter, dimensions);
            case IN:
                InFilter inFilter = (InFilter) filter;
                return evaluate(builder, inFilter, dimensions);
        }

        throw new UnsupportedOperationException("Can't evaluate filter " + filter);
    }

    /**
     * Evaluates a regular expression filter.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param regexFilter  A regexFilter to be evaluated.
     * @param dimensions  The list of dimensions already found.
     *
     * @return a RexNode containing an equivalent filter to the one given.
     */
    private static RexNode evaluate(RelBuilder builder, RegularExpressionFilter regexFilter, List<String> dimensions) {
        //todo test this
        String apiName = regexFilter.getDimension().getApiName();
        dimensions.add(apiName);
        return builder.call(
                SqlStdOperatorTable.LIKE,
                builder.field(apiName),
                builder.literal(regexFilter.getPattern().toString())
        );
    }

    /**
     * Evaluates a Selector filter.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param selectorFilter  A selectorFilter to be evaluated.
     * @param dimensions  The list of dimensions already found.
     *
     * @return a RexNode containing an equivalent filter to the one given.
     */
    private static RexNode evaluate(RelBuilder builder, SelectorFilter selectorFilter, List<String> dimensions) {
        String apiName = selectorFilter.getDimension().getApiName();
        dimensions.add(apiName);
        return builder.call(
                SqlStdOperatorTable.EQUALS,
                builder.field(apiName),
                builder.literal(selectorFilter.getValue())
        );
    }

    /**
     * Evaluates a SearchFilter filter. Currently doesn't support Fragment mode.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param searchFilter  A searchFilter to be evaluated.
     * @param dimensions  The list of dimensions already found.
     *
     * @return a RexNode containing an equivalent filter to the one given.
     */
    private static RexNode evaluate(RelBuilder builder, SearchFilter searchFilter, List<String> dimensions) {
        // todo put these in SearchFilter?
        String typeKey = "type";
        String valueKey = "value";

        String searchType = searchFilter.getQuery().get(typeKey);
        SearchFilter.QueryType queryType = SearchFilter.QueryType.fromType(searchType);

        String columnName = searchFilter.getDimension().getApiName();
        dimensions.add(columnName);
        String valueToFind = searchFilter.getQuery().get(valueKey);

        switch (queryType) {
            case Contains:
                return builder.call(
                        SqlStdOperatorTable.LIKE,
                        builder.field(columnName),
                        builder.literal("%" + valueToFind + "%")
                );
            case InsensitiveContains:
                // todo maybe look at SqlCollation
                return builder.call(
                        SqlStdOperatorTable.LIKE,
                        builder.call(
                                SqlStdOperatorTable.LOWER,
                                builder.field(columnName)
                        ),
                        builder.literal("%" + valueToFind.toLowerCase(Locale.ENGLISH) + "%")
                );
            case Fragment:
                // todo: fragment takes json array of strings and checks if any are contained? just OR search over them?
                // http://druid.io/docs/0.9.1.1/querying/filters.html
            default:
                throw new UnsupportedOperationException("Not implemented");
        }
    }

    /**
     * Evaluates an Infilter filter.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param inFilter  An inFilter to be evaluated.
     * @param dimensions  The list of dimensions already found.
     *
     * @return a RexNode containing an equivalent filter to the one given.
     */
    private static RexNode evaluate(RelBuilder builder, InFilter inFilter, List<String> dimensions) {
        Dimension dimension = inFilter.getDimension();
        dimensions.add(dimension.getApiName());

        OrFilter orFilterOfSelectors = new OrFilter(
                inFilter.getValues()
                        .stream()
                        .map(value -> new SelectorFilter(dimension, value))
                        .collect(Collectors.toList())
        );
        return evaluate(builder, orFilterOfSelectors, dimensions);
    }

    /**
     * Evaluates a complex filter by performing a {@link SqlOperator} over a list of dimensions.
     *
     * @param builder  The RelBuilder used with Calcite to make queries.
     * @param complexFilter  A complexFilter to be evaluated.
     * @param dimensions  The list of dimensions already found.
     * @param operator  The sql operator to be applied to a complexFilter's fields.
     *
     * @return a RexNode containing an equivalent filter to the one given.
     */
    private static RexNode listEvaluate(
            RelBuilder builder,
            ComplexFilter complexFilter,
            List<String> dimensions,
            SqlOperator operator
    ) {
        List<RexNode> rexNodes = complexFilter.getFields()
                .stream()
                .map(filter -> evaluate(builder, filter, dimensions))
                .collect(Collectors.toList());

        return builder.call(
                operator,
                rexNodes
        );
    }
}
