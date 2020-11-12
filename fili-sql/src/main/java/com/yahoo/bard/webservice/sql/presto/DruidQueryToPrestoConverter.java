// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.presto;

import static com.yahoo.bard.webservice.druid.model.DefaultQueryType.GROUP_BY;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec;
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery;
import com.yahoo.bard.webservice.sql.ApiToFieldMapper;
import com.yahoo.bard.webservice.sql.DruidQueryToSqlConverter;
import com.yahoo.bard.webservice.sql.aggregation.DruidSqlAggregationConverter;
import com.yahoo.bard.webservice.sql.aggregation.SqlAggregation;
import com.yahoo.bard.webservice.sql.evaluator.FilterEvaluator;
import com.yahoo.bard.webservice.sql.evaluator.PrestoFilterEvaluator;
import com.yahoo.bard.webservice.sql.helper.CalciteHelper;
import com.yahoo.bard.webservice.sql.helper.SqlTimeConverter;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Default implementation of converting a {@link DruidQuery} into a sql query.
 */
public class DruidQueryToPrestoConverter extends DruidQueryToSqlConverter {
    private static final Logger LOG = LoggerFactory.getLogger(DruidQueryToPrestoConverter.class);

    /**
     * Constructs the default converter.
     *
     * TODO could make an interface with {@link #isValidQuery(DruidQuery)} and
     * {@link #buildSqlQuery(DruidAggregationQuery, ApiToFieldMapper)}. Maybe make it a generic
     * more generic like "{@code DruidQueryConverter<T>}"
     *
     * @param calcitePrestoHelper  The calcite presto helper for this database.
     */
    public DruidQueryToPrestoConverter(CalciteHelper calcitePrestoHelper) {
        super(calcitePrestoHelper);
    }

    /**
     * Builds a time converter to designating how to translate between druid and sql
     * time information.
     *
     * @return a new time converter.
     */
    protected SqlTimeConverter buildSqlTimeConverter() {
        return new SqlTimeConverter("yyyyMMddHHmmss");
    }

    /**
     * Builds a filterEvaluator.
     *
     * @return a filter evaluator
     */
    protected FilterEvaluator buildFilterEvaluator() {
        return new PrestoFilterEvaluator();
    }

    /**
     * Builds a converter between druid and sql aggregations.
     *
     * @return a new druid to sql aggregation converter.
     */
    protected BiFunction<Aggregation, ApiToFieldMapper, Optional<SqlAggregation>> buildDruidSqlTypeConverter() {
        return new DruidSqlAggregationConverter();
    }

    /**
     * Finds the sorting for a druid query.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The query to find the sorting from.
     * @param apiToFieldMapper  The mapping from api to physical names.
     * @param timestampColumn  The name of the timestamp column in the database.
     *
     * @return a collection of rexnodes to apply sorts in calcite.
     */
    @Override
    protected List<RexNode> getSort(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper,
            String timestampColumn
    ) {
        // druid does NULLS FIRST
        List<RexNode> sorts = new ArrayList<>();
        int timePartFunctions = getTimeConverter().timeGrainToDatePartFunctions(druidQuery.getGranularity()).size();
        int groupBys = druidQuery.getDimensions().size() + timePartFunctions;

        List<RexNode> limitSpecSorts = new ArrayList<>();
        Set<String> limitSpecColumns = new HashSet<>();
        if (druidQuery.getQueryType().equals(GROUP_BY)) {
            GroupByQuery groupByQuery = (GroupByQuery) druidQuery;
            LimitSpec limitSpec = groupByQuery.getLimitSpec();
            if (limitSpec != null) {
                limitSpec.getColumns()
                        .stream()
                        .map(orderByColumn -> {
                            // TODO: investigate the field name mapping discrepency
                            String orderByField = apiToFieldMapper.unApply(orderByColumn.getDimension());
                            limitSpecColumns.add(orderByField);
                            RexNode sort = builder.literal(orderByField);
                            if (orderByColumn.getDirection().equals(SortDirection.DESC)) {
                                sort = builder.desc(sort);
                            }
                            return sort;
                        })
                        .forEach(limitSpecSorts::add);
            }
        }
        // add time group by
        if (timePartFunctions == 0) {
            sorts.add(builder.field(timestampColumn));
        }
        sorts.addAll(builder.fields().subList(druidQuery.getDimensions().size(), groupBys));

        // add limit spec group by
        sorts.addAll(limitSpecSorts);

        // add remaining group by
        List<RexNode> unorderedDimensions = druidQuery.getDimensions().stream()
                .map(Dimension::getApiName)
                .map(apiToFieldMapper)
                .filter(columnName -> !limitSpecColumns.contains(columnName))
                .map(builder::field)
                .collect(Collectors.toList());
        sorts.addAll(unorderedDimensions);

        return sorts.stream()
                .map(sort -> builder.call(SqlStdOperatorTable.NULLS_FIRST, sort))
                .collect(Collectors.toList());
    }

    /**
     * Determines whether or not a query is able to be processed using
     * the Sql backend.
     *
     * @param druidQuery  The query to check if is able to be processed.
     *
     * @return true if a valid query, else false.
     */
    protected boolean isValidQuery(DruidQuery<?> druidQuery) {
        return super.isValidQuery(druidQuery);
    }
}
