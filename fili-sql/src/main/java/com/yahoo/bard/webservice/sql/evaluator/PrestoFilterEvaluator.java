// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator;

import com.yahoo.bard.webservice.druid.model.filter.*;
import com.yahoo.bard.webservice.sql.ApiToFieldMapper;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;
import java.util.stream.Collectors;

import static com.yahoo.bard.webservice.druid.model.filter.Filter.DefaultFilterType.*;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LIKE;

public class PrestoFilterEvaluator extends FilterEvaluator {
    @Override
    public RexNode evaluateFilter(Filter filter, RelBuilder builder, ApiToFieldMapper apiToFieldMapper) {
        // TODO: investigate possibility of removing cast. E.g. fetch field type and cast filter value to the type
        FilterType type = filter.getType();
        if (type == SEARCH) {
            SqlOperator operator = LIKE;
            String filterValue = new StringBuilder()
                    .append("%")
                    .append(((SearchFilter) filter).getQueryValue())
                    .append("%")
                    .toString();
            if (filterValue.equals("%%")) {
                filterValue = "";
            }
            String filterDimApiName = ((SearchFilter) filter).getDimension().getApiName();
            String filterDimFieldName = apiToFieldMapper.apply(filterDimApiName);
            return builder.call(
                    operator,
                    builder.cast(builder.field(filterDimFieldName), SqlTypeName.VARCHAR),
                    builder.literal(filterValue)
            );
        } else if (type == SELECTOR) {
            SqlOperator operator = EQUALS;
            String filterValue = ((SelectorFilter) filter).getValue();
            String filterDimApiName = ((SelectorFilter) filter).getDimension().getApiName();
            String filterDimFieldName = apiToFieldMapper.apply(filterDimApiName);
            return builder.call(
                    operator,
                    builder.cast(builder.field(filterDimFieldName), SqlTypeName.VARCHAR),
                    builder.literal(filterValue)
            );
        } else if (type == OR) {
            List<RexNode> orConditions = ((OrFilter) filter).getFields().stream()
                    .map(filter1 -> evaluateFilter(filter1, builder, apiToFieldMapper))
                    .collect(Collectors.toList());
            return builder.or(orConditions);
        } else if (type == AND) {
            List<RexNode> andConditions = ((AndFilter) filter).getFields().stream()
                    .map(filter1 -> evaluateFilter(filter1, builder, apiToFieldMapper))
                    .collect(Collectors.toList());
            return builder.and(andConditions);
        } else if (type == NOT) {
            List<RexNode> notConditions = ((NotFilter) filter).getFields().stream()
                    .map(filter1 -> evaluateFilter(filter1, builder, apiToFieldMapper))
                    .collect(Collectors.toList());
            return builder.not(notConditions.get(0));
        } else {
            throw new IllegalStateException("Cannot handle FilterType: " + type);
        }
    }
}
