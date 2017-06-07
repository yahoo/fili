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

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by hinterlong on 6/6/17.
 */
public class FilterEvaluator {
    private FilterEvaluator() {

    }

    public static void add(RelBuilder builder, Filter filter) {
        if (filter == null) {
            return;
        }

        builder.filter(
                evaluate(builder, filter)
        );
    }

    private static RexNode evaluate(RelBuilder builder, Filter filter) {
        DefaultFilterType defaultFilterType = (DefaultFilterType) filter.getType();
        switch (defaultFilterType) {
            case SELECTOR:
                SelectorFilter selectorFilter = (SelectorFilter) filter;
                return evaluate(builder, selectorFilter);
            case REGEX:
                RegularExpressionFilter regexFilter = (RegularExpressionFilter) filter;
                return evaluate(builder, regexFilter);
            case AND:
                return listEvaluate(builder, (ComplexFilter) filter, SqlStdOperatorTable.AND);
            case OR:
                return listEvaluate(builder, (ComplexFilter) filter, SqlStdOperatorTable.OR);
            case NOT:
                return listEvaluate(builder, (ComplexFilter) filter, SqlStdOperatorTable.NOT);
            case EXTRACTION:
                throw new UnsupportedOperationException(
                        "Not implemented. Also deprecated use selector filter with extraction function");
            case SEARCH:
                SearchFilter searchFilter = (SearchFilter) filter;
                return evaluate(builder, searchFilter);
            case IN:
                InFilter inFilter = (InFilter) filter;
                return evaluate(builder, inFilter);
        }

        throw new UnsupportedOperationException("Can't evaluate filter " + filter);
    }

    private static RexNode evaluate(RelBuilder builder, RegularExpressionFilter regexFilter) {
        //todo test this
        return builder.call(
                SqlStdOperatorTable.LIKE,
                builder.field(regexFilter.getDimension().getApiName()),
                builder.literal(regexFilter.getPattern().toString())
        );
    }

    private static RexNode evaluate(RelBuilder builder, SelectorFilter selectorFilter) {
        return builder.call(
                SqlStdOperatorTable.EQUALS,
                builder.field(selectorFilter.getDimension().getApiName()),
                builder.literal(selectorFilter.getValue())
        );
    }

    private static RexNode evaluate(RelBuilder builder, SearchFilter searchFilter) {
        // todo: fragment takes json array of strings and checks if any are contained? just OR search over them?
        // http://druid.io/docs/0.9.1.1/querying/filters.html
        // todo put these in SearchFilter?
        String typeKey = "type";
        String valueKey = "value";

        String searchType = searchFilter.getQuery().get(typeKey);
        SearchFilter.QueryType queryType = SearchFilter.QueryType.fromType(searchType);

        String columnName = searchFilter.getDimension().getApiName();
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

    private static RexNode evaluate(RelBuilder builder, InFilter inFilter) {
        Dimension dimension = inFilter.getDimension();

        OrFilter orFilterOfSelectors = new OrFilter(
                inFilter.getValues()
                        .stream()
                        .map(value -> new SelectorFilter(dimension, value))
                        .collect(Collectors.toList())
        );
        return evaluate(builder, orFilterOfSelectors);
    }

    private static RexNode listEvaluate(RelBuilder builder, ComplexFilter complexFilter, SqlOperator operator) {
        List<RexNode> rexNodes = complexFilter.getFields()
                .stream()
                .filter(Objects::nonNull)
                .map(filter -> evaluate(builder, filter))
                .collect(Collectors.toList());

        return builder.call(
                operator,
                rexNodes
        );
    }

    public static Collection<String> getDimensionNames(Filter filter) {
        // todo get dimensions from filters
        return Arrays.asList("COMMENT");
    }
}
