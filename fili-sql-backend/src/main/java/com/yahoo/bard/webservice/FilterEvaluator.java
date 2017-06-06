package com.yahoo.bard.webservice;

import com.yahoo.bard.webservice.druid.model.filter.ComplexFilter;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType;
import com.yahoo.bard.webservice.druid.model.filter.InFilter;
import com.yahoo.bard.webservice.druid.model.filter.SearchFilter;
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by hinterlong on 6/6/17.
 */
public class FilterEvaluator {
    private FilterEvaluator() {

    }

    public void add(RelBuilder builder, Filter filter) {
        if (filter == null) {
            return;
        }

        builder.filter(
                evaluate(builder, filter)
        );
    }

    private RexNode evaluate(RelBuilder builder, Filter filter) {
        DefaultFilterType defaultFilterType = (DefaultFilterType) filter.getType();
        switch (defaultFilterType) {
            case SELECTOR:
                SelectorFilter selectorFilter = (SelectorFilter) filter;
                return evaluate(builder, selectorFilter);
            case REGEX:
                throw new UnsupportedOperationException("Not implemented");
            case AND:
                return listEvaluate(builder, (ComplexFilter) filter, SqlStdOperatorTable.AND);
            case OR:
                return listEvaluate(builder, (ComplexFilter) filter, SqlStdOperatorTable.OR);
            case NOT:
                return listEvaluate(builder, (ComplexFilter) filter, SqlStdOperatorTable.NOT);
            case EXTRACTION:
                throw new UnsupportedOperationException("Not implemented");
            case SEARCH:
                SearchFilter searchFilter = (SearchFilter) filter;
                evaluate(builder, searchFilter);
            case IN:
                InFilter inFilter = (InFilter) filter;
                return evaluate(builder, inFilter);
        }

        throw new UnsupportedOperationException("Can't evaluate filter " + filter);
    }

    private RexNode evaluate(RelBuilder builder, SelectorFilter selectorFilter) {
        return builder.call(
                SqlStdOperatorTable.EQUALS,
                builder.field(selectorFilter.getDimension().getApiName()),
                builder.literal(selectorFilter.getValue())
        );
    }

    private RexNode evaluate(RelBuilder builder, SearchFilter searchFilter) {
        // todo: not sure for insensitive, what is fragment
        // https://stackoverflow.com/questions/2876789/how-can-i-search-case-insensitive-in-a-column-using-like-wildcard
        String type = searchFilter.getQuery().keySet().stream().findFirst().get();
        String value = searchFilter.getQuery().get(type);

        SearchFilter.QueryType queryType = SearchFilter.QueryType.valueOf(type);
        switch (queryType) {
            case Contains:
                return builder.call(
                        SqlStdOperatorTable.LIKE,
                        builder.field(searchFilter.getDimension().getApiName()),
                        builder.literal("%" + value + "%")
                );
            case InsensitiveContains:
            case Fragment:
            default:
                throw new UnsupportedOperationException("Not implemented");
        }
    }

    private RexNode evaluate(RelBuilder builder, InFilter inFilter) {
        throw new UnsupportedOperationException("Not implemented");
    }

    private RexNode listEvaluate(RelBuilder builder, ComplexFilter complexFilter, SqlOperator operator) {
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
}
