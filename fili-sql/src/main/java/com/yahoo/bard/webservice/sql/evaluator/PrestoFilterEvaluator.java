// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator;

import com.yahoo.bard.webservice.druid.model.filter.*;
import com.yahoo.bard.webservice.sql.ApiToFieldMapper;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import java.util.Locale;


public class PrestoFilterEvaluator extends FilterEvaluator {

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
                builder.cast(builder.field(apiToFieldMapper.apply(apiName)), SqlTypeName.VARCHAR),
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
    @Override
    public RexNode evaluate(SearchFilter searchFilter, RelBuilder builder, ApiToFieldMapper apiToFieldMapper) {
        String searchType = searchFilter.getQueryType();
        SearchFilter.QueryType optionalQuery = SearchFilter.QueryType.fromType(searchType)
                .orElseThrow(() -> new IllegalArgumentException("Couldn't convert " + searchType + " to a QueryType."));

        String columnName = searchFilter.getDimension().getApiName();
        String valueToFind = searchFilter.getQueryValue();

        switch (optionalQuery) {
            case Contains:
                String filterLiteral = "%" + valueToFind + "%";
                if (valueToFind.isEmpty()) {
                    filterLiteral = "";
                }
                return builder.call(
                        SqlStdOperatorTable.LIKE,
                        builder.cast(builder.field(apiToFieldMapper.apply(columnName)), SqlTypeName.VARCHAR),
                        builder.literal(filterLiteral)
                );
            case InsensitiveContains:
                // todo maybe look at SqlCollation
                return builder.call(
                        SqlStdOperatorTable.LIKE,
                        builder.call(
                                SqlStdOperatorTable.LOWER,
                                builder.cast(builder.field(apiToFieldMapper.apply(columnName)), SqlTypeName.VARCHAR)
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
}
