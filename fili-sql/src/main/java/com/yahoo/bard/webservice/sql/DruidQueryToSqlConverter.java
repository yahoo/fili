// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import static com.yahoo.bard.webservice.druid.model.DefaultQueryType.GROUP_BY;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.QueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec;
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery;
import com.yahoo.bard.webservice.sql.aggregation.DruidSqlAggregationConverter;
import com.yahoo.bard.webservice.sql.aggregation.SqlAggregation;
import com.yahoo.bard.webservice.sql.evaluator.FilterEvaluator;
import com.yahoo.bard.webservice.sql.evaluator.HavingEvaluator;
import com.yahoo.bard.webservice.sql.helper.CalciteHelper;
import com.yahoo.bard.webservice.sql.helper.SqlTimeConverter;
import com.yahoo.bard.webservice.table.SqlPhysicalTable;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Default implementation of converting a {@link DruidQuery} into a sql query.
 */
public class DruidQueryToSqlConverter {
    private static final Logger LOG = LoggerFactory.getLogger(DruidQueryToSqlConverter.class);
    private final CalciteHelper calciteHelper;
    private final SqlTimeConverter sqlTimeConverter;
    private final BiFunction<Aggregation, ApiToFieldMapper, Optional<SqlAggregation>> druidSqlAggregationConverter;

    /**
     * Constructs the default converter.
     *
     * TODO could make an interface with {@link #isValidQuery(DruidQuery)} and
     * {@link #buildSqlQuery(DruidAggregationQuery, ApiToFieldMapper)}. Maybe make it a generic
     * more generic like "{@code DruidQueryConverter<T>}"
     *
     * @param calciteHelper  The calcite helper for this database.
     */
    public DruidQueryToSqlConverter(CalciteHelper calciteHelper) {
        this.calciteHelper = calciteHelper;
        this.sqlTimeConverter = buildSqlTimeConverter();
        this.druidSqlAggregationConverter = buildDruidSqlTypeConverter();
    }

    /**
     * Builds a time converter to designating how to translate between druid and sql
     * time information.
     *
     * @return a new time converter.
     */
    protected SqlTimeConverter buildSqlTimeConverter() {
        return new SqlTimeConverter();
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
     * Determines whether or not a query is able to be processed using
     * the Sql backend.
     *
     * @param druidQuery  The query to check if is able to be processed.
     *
     * @return true if a valid query, else false.
     */
    protected boolean isValidQuery(DruidQuery<?> druidQuery) {
        QueryType queryType = druidQuery.getQueryType();
        LOG.debug("Processing {} query\n {}", queryType, druidQuery);

        if (queryType instanceof DefaultQueryType) {
            DefaultQueryType defaultQueryType = (DefaultQueryType) queryType;
            switch (defaultQueryType) {
                case TIMESERIES:
                case GROUP_BY:
                    return true;
            }
        }

        return false;
    }

    /**
     * Builds the druid query as sql and returns it as a string.
     *
     * @param druidQuery  The query to convert to sql.
     * @param apiToFieldMapper  The mapping between api and physical names for the query.
     *
     * @return the sql equivalent of the query.
     */
    public String buildSqlQuery(DruidAggregationQuery<?> druidQuery, ApiToFieldMapper apiToFieldMapper) {
        SqlPhysicalTable sqlTable = (SqlPhysicalTable) druidQuery.getDataSource()
                .getPhysicalTable()
                .getSourceTable();

        LOG.debug(
                "Querying table {} with schema {} using timestampColumn {}",
                sqlTable.getName(),
                sqlTable.getSchemaName(),
                sqlTable.getTimestampColumn()
        );

        RelNode query = convertDruidQueryToRelNode(druidQuery, apiToFieldMapper, sqlTable);
        RelToSqlConverter relToSql = calciteHelper.getNewRelToSqlConverter();
        SqlPrettyWriter sqlWriter = calciteHelper.getNewSqlWriter();

        return writeSql(sqlWriter, relToSql, query);
    }

    /**
     * Converts the druid query to a {@link RelNode}.
     *
     * @param druidQuery  The query to convert to sql.
     * @param apiToFieldMapper  The mapping between api and physical names for the query.
     * @param sqlTable  The sql table being queried against.
     *
     * @return the sql equivalent of the query.
     */
    private RelNode convertDruidQueryToRelNode(
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper,
            SqlPhysicalTable sqlTable
    ) {
        RelBuilder builder = calciteHelper.getNewRelBuilder(sqlTable.getSchemaName());
        return builder.scan(sqlTable.getName())
                .filter(
                        getAllWhereFilters(builder, druidQuery, apiToFieldMapper, sqlTable.getTimestampColumn())
                )
                .aggregate(
                        builder.groupKey(getAllGroupByColumns(
                                builder,
                                druidQuery,
                                apiToFieldMapper,
                                sqlTable.getTimestampColumn()
                        )),
                        getAllQueryAggregations(builder, druidQuery, apiToFieldMapper)
                )
                .filter(
                        getHavingFilter(builder, druidQuery, apiToFieldMapper)
                )
                .sort(
                        getSort(builder, druidQuery, apiToFieldMapper, sqlTable.getTimestampColumn())
                )
                .build();
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
    protected List<RexNode> getSort(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper,
            String timestampColumn
    ) {
        // druid does NULLS FIRST
        List<RexNode> sorts = new ArrayList<>();
        int timePartFunctions = sqlTimeConverter.timeGrainToDatePartFunctions(druidQuery.getGranularity()).size();
        int groupBys = druidQuery.getDimensions().size() + timePartFunctions;

        List<RexNode> metricSorts = new ArrayList<>();
        if (druidQuery.getQueryType().equals(GROUP_BY)) {
            GroupByQuery groupByQuery = (GroupByQuery) druidQuery;
            LimitSpec limitSpec = groupByQuery.getLimitSpec();
            if (limitSpec != null) {
                limitSpec.getColumns()
                        .stream()
                        .map(orderByColumn -> {
                            RexNode sort = builder.field(orderByColumn.getDimension());
                            if (orderByColumn.getDirection().equals(SortDirection.DESC)) {
                                sort = builder.desc(sort);
                            }
                            return sort;
                        })
                        .forEach(metricSorts::add);
            }
        }

        if (timePartFunctions == 0) {
            sorts.add(builder.field(timestampColumn));
        }
        sorts.addAll(builder.fields().subList(druidQuery.getDimensions().size(), groupBys));
        sorts.addAll(metricSorts);
        sorts.addAll(getDimensionFields(builder, druidQuery, apiToFieldMapper));

        return sorts.stream()
                .map(sort -> builder.call(SqlStdOperatorTable.NULLS_FIRST, sort))
                .collect(Collectors.toList());
    }

    /**
     * Gets all the dimensions from a druid query as fields for calcite.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The query to find the having filter from.
     * @param apiToFieldMapper  The mapping from api to physical name.
     *
     * @return the list of dimensions as {@link RexNode} for Calcite's builder.
     */
    private List<RexNode> getDimensionFields(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper
    ) {
        return druidQuery.getDimensions().stream()
                .map(Dimension::getApiName)
                .map(apiToFieldMapper)
                .map(builder::field)
                .collect(Collectors.toList());
    }

    /**
     * Returns the RexNode used to filter the druidQuery.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The query from which to find filter all the filters for.
     * @param apiToFieldMapper  The mapping from api to physical names.
     * @param timestampColumn  The name of the timestamp column in the database.
     *
     * @return the combined RexNodes that should be filtered on.
     */
    protected RexNode getAllWhereFilters(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper,
            String timestampColumn
    ) {
        RexNode timeFilter = sqlTimeConverter.buildTimeFilters(
                builder,
                druidQuery,
                timestampColumn
        );

        if (druidQuery.getFilter() != null) {
            FilterEvaluator filterEvaluator = new FilterEvaluator();
            RexNode druidQueryFilter = filterEvaluator.evaluateFilter(
                    builder,
                    druidQuery.getFilter(),
                    apiToFieldMapper
            );
            return builder.and(timeFilter, druidQueryFilter);
        }

        return timeFilter;
    }

    /**
     * Gets the collection of having filters to be applied from the druid query.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The query to find the having filter from.
     * @param apiToFieldMapper  The mapping from api to physical name.
     *
     * @return the collection of equivalent filters for calcite.
     */
    protected Collection<RexNode> getHavingFilter(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper
    ) {
        RexNode filter = null;
        if (druidQuery.getQueryType().equals(GROUP_BY)) {
            Having having = ((GroupByQuery) druidQuery).getHaving();

            if (having != null) {
                HavingEvaluator havingEvaluator = new HavingEvaluator();
                filter = havingEvaluator.evaluateHaving(builder, having, apiToFieldMapper);
            }
        }

        return Collections.singletonList(filter);
    }

    /**
     * Find all druid aggregations and convert them to {@link org.apache.calcite.tools.RelBuilder.AggCall}.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The druid query to get the aggregations of.
     * @param apiToFieldMapper  The mapping from api to physical name.
     *
     * @return the list of aggregations.
     */
    protected List<RelBuilder.AggCall> getAllQueryAggregations(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper
    ) {
        return druidQuery.getAggregations()
                .stream()
                .map(aggregation -> druidSqlAggregationConverter.apply(aggregation, apiToFieldMapper))
                .filter(sqlAggregation -> {
                    if (!sqlAggregation.isPresent()) {
                        String msg = "Couldn't build sql aggregation with " + sqlAggregation;
                        LOG.debug(msg);
                        throw new RuntimeException(msg);
                    }
                    return true;
                })
                .map(Optional::get)
                .map(sqlAggregation -> builder.aggregateCall(
                        sqlAggregation.getSqlAggFunction(),
                        false,
                        null,
                        sqlAggregation.getSqlAggregationAsName(),
                        builder.field(sqlAggregation.getSqlAggregationFieldName())
                ))
                .collect(Collectors.toList());
    }

    /**
     * Collects all the time columns and dimensions to be grouped on.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The query to find grouping columns from.
     * @param apiToFieldMapper  The mapping from api to physical name.
     * @param timestampColumn  The name of the timestamp column in the database.
     *
     * @return all columns which should be grouped on.
     */
    protected List<RexNode> getAllGroupByColumns(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper,
            String timestampColumn
    ) {
        List<RexNode> timeFilters = sqlTimeConverter.buildGroupBy(
                builder,
                druidQuery.getGranularity(),
                timestampColumn
        );

        List<RexNode> dimensionFields = getDimensionFields(builder, druidQuery, apiToFieldMapper);

        List<RexNode> allGroupBys = new ArrayList<>();
        allGroupBys.addAll(timeFilters);
        allGroupBys.addAll(dimensionFields);
        return allGroupBys;
    }

    /**
     * Converts a RelBuilder into a sql string.
     *
     * @param sqlWriter  The writer to be used when translating the {@link org.apache.calcite.rel.RelNode} to sql.
     * @param relToSql  The converter from {@link org.apache.calcite.rel.RelNode} to
     * {@link org.apache.calcite.sql.SqlNode}.
     * @param query  The RelNode representing the query.
     *
     * @return the sql string built by the RelBuilder.
     */
    protected String writeSql(SqlPrettyWriter sqlWriter, RelToSqlConverter relToSql, RelNode query) {
        sqlWriter.reset();
        SqlSelect select = relToSql.visitChild(0, query).asSelect();
        return sqlWriter.format(select);
    }

    public SqlTimeConverter getTimeConverter() {
        return sqlTimeConverter;
    }
}
