// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.filter.ComplexFilter;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType;
import com.yahoo.bard.webservice.druid.model.filter.InFilter;
import com.yahoo.bard.webservice.druid.model.filter.OrFilter;
import com.yahoo.bard.webservice.druid.model.filter.RegularExpressionFilter;
import com.yahoo.bard.webservice.druid.model.filter.SearchFilter;
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter;
import com.yahoo.bard.webservice.util.EnumUtils;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by hinterlong on 6/6/17.
 */
public class FilterEvaluator {
    private FilterEvaluator() {

    }

    public static List<String> getDimensionNames(RelBuilder builder, Filter filter) {
        Pair<RexNode, List<String>> filterAndDimensions = evaluate(builder, filter);
        return filterAndDimensions.getRight();
    }

    public static void addFilter(RelBuilder builder, Filter filter) {
        // todo look at caller, this may be moved there
        Pair<RexNode, List<String>> filterAndDimensions = evaluate(builder, filter);
        if (filter != null) {
            builder.filter(
                    filterAndDimensions.getLeft()
            );
        }
    }

    private static Pair<RexNode, List<String>> evaluate(RelBuilder builder, Filter filter) {
        List<String> dimensions = new ArrayList<>();
        RexNode rexNode = evaluate(builder, filter, dimensions);
        dimensions = dimensions.stream().distinct().collect(Collectors.toList());
        return Pair.of(rexNode, dimensions);
    }

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

    private static RexNode evaluate(RelBuilder builder, SelectorFilter selectorFilter, List<String> dimensions) {
        String apiName = selectorFilter.getDimension().getApiName();
        dimensions.add(apiName);
        return builder.call(
                SqlStdOperatorTable.EQUALS,
                builder.field(apiName),
                builder.literal(selectorFilter.getValue())
        );
    }

    private static RexNode evaluate(RelBuilder builder, SearchFilter searchFilter, List<String> dimensions) {
        // todo: fragment takes json array of strings and checks if any are contained? just OR search over them?
        // http://druid.io/docs/0.9.1.1/querying/filters.html
        // todo put these in SearchFilter?
        String typeKey = "type";
        String valueKey = "value";

        String searchType = searchFilter.getQuery().get(typeKey);
        SearchFilter.QueryType queryType = fromType(searchType);

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
                return builder.call(
                        SqlStdOperatorTable.LIKE,
                        builder.call(
                                SqlStdOperatorTable.LOWER,
                                builder.field(columnName)
                        ),
                        builder.literal("%" + valueToFind.toLowerCase() + "%")
                );
            case Fragment:
            default:
                throw new UnsupportedOperationException("Not implemented");
        }
    }

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

    private static RexNode listEvaluate(
            RelBuilder builder,
            ComplexFilter complexFilter,
            List<String> dimensions,
            SqlOperator operator
    ) {
        List<RexNode> rexNodes = complexFilter.getFields()
                .stream()
                .filter(Objects::nonNull)
                .map(filter -> evaluate(builder, filter, dimensions))
                .collect(Collectors.toList());

        return builder.call(
                operator,
                rexNodes
        );
    }

    /**
     * Get the QueryType enum fromType it's search type.
     * @param type  Type of the query type (for serialization)
     * @return the enum QueryType
     */
    public static SearchFilter.QueryType fromType(String type) {
        // todo this belongs in SearchFilter
        for (SearchFilter.QueryType queryType : SearchFilter.QueryType.values()) {
            if (queryType.toString().equalsIgnoreCase(EnumUtils.camelCase(type))) {
                return queryType;
            }
        }
        throw new IllegalArgumentException("No query type corresponds to " + type);
    }
}
