// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec;
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery;
import com.yahoo.bard.webservice.sql.aggregation.DefaultDruidSqlAggregationConverter;
import com.yahoo.bard.webservice.sql.aggregation.DruidSqlAggregationConverter;
import com.yahoo.bard.webservice.sql.evaluator.FilterEvaluator;
import com.yahoo.bard.webservice.sql.evaluator.HavingEvaluator;
import com.yahoo.bard.webservice.sql.helper.CalciteHelper;
import com.yahoo.bard.webservice.sql.helper.DatabaseHelper;
import com.yahoo.bard.webservice.sql.helper.DefaultSqlTimeConverter;
import com.yahoo.bard.webservice.sql.helper.SqlTimeConverter;

import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Default implementation of converting a {@link DruidQuery} into a sql query.
 */
public class DruidQueryToSqlConverter {
    private static final Logger LOG = LoggerFactory.getLogger(DruidQueryToSqlConverter.class);
    private final CalciteHelper calciteHelper;
    private final SqlTimeConverter sqlTimeConverter;
    private final DruidSqlAggregationConverter druidSqlAggregationConverter;

    /**
     * Constructs the default converter.
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
        return new DefaultSqlTimeConverter();
    }

    /**
     * Builds a converter between druid and sql aggregations.
     *
     * @return a new druid to sql aggregation converter.
     */
    protected DruidSqlAggregationConverter buildDruidSqlTypeConverter() {
        return new DefaultDruidSqlAggregationConverter();
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
        DefaultQueryType queryType = (DefaultQueryType) druidQuery.getQueryType();
        LOG.debug("Processing {} query\n {}", queryType, druidQuery);

        switch (queryType) {
            case TIMESERIES:
            case GROUP_BY:
                return true;
        }

        return false;
    }

    /**
     * Builds the druid query as sql and returns it as a string.
     *
     * @param connection  The connection to the database.
     * @param druidQuery  The query to convert to sql.
     * @param apiToFieldMapper  The mapping between api and physical names for the query.
     *
     * @return the sql equivalent of the query.
     *
     * @throws SQLException if can't connect to database.
     */
    public String buildSqlQuery(
            Connection connection,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper apiToFieldMapper
    ) throws SQLException {
        String sqlTableName = druidQuery.getDataSource().getPhysicalTable().getName();
        String timestampColumn = DatabaseHelper.getTimestampColumn(
                connection,
                calciteHelper.getSchemaName(),
                sqlTableName
        );

        LOG.info("Using timestamp column of '{}' for table {}", timestampColumn, sqlTableName);

        RelBuilder builder = calciteHelper.getNewRelBuilder();
        builder.scan(sqlTableName)
                .filter(
                        getAllWhereFilters(
                                builder,
                                druidQuery,
                                timestampColumn
                        )
                )
                .aggregate(
                        builder.groupKey(
                                getAllGroupByColumns(builder, druidQuery, timestampColumn)
                        ),
                        getAllQueryAggregations(builder, druidQuery, apiToFieldMapper)
                )
                .filter(
                        getHavingFilter(builder, druidQuery, apiToFieldMapper)
                )
                .sort(
                        getSort(builder, druidQuery, apiToFieldMapper, timestampColumn)
                );
        RelToSqlConverter relToSql = calciteHelper.getNewRelToSqlConverter();
        SqlPrettyWriter sqlWriter = calciteHelper.getNewSqlWriter();

        return writeSql(sqlWriter, relToSql, builder);
    }

    /**
     * Finds the sorting for a druid query.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The query to find the sorting from.
     * @param aliasMaker  The mapping from api to physical names.
     * @param timestampColumn  The name of the timestamp column in the database.
     *
     * @return a collection of rexnodes to apply sorts in calcite.
     */
    protected List<RexNode> getSort(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper aliasMaker,
            String timestampColumn
    ) {
        // druid does NULLS FIRST
        List<RexNode> sorts = new ArrayList<>();
        int timePartFunctions = sqlTimeConverter.getNumberOfGroupByFunctions(druidQuery.getGranularity());
        int groupBys = druidQuery.getDimensions().size() + timePartFunctions;

        List<RexNode> metricSorts = new ArrayList<>();
        if (druidQuery.getQueryType().equals(DefaultQueryType.GROUP_BY)) {
            GroupByQuery groupByQuery = (GroupByQuery) druidQuery;
            LimitSpec limitSpec = groupByQuery.getLimitSpec();
            if (limitSpec != null) {
                limitSpec.getColumns()
                        .stream()
                        .map(orderByColumn -> {
                            RexNode sort = builder.field(aliasMaker.unApply(orderByColumn.getDimension()));
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
        sorts.addAll(builder.fields().subList(0, druidQuery.getDimensions().size()));
        sorts.addAll(metricSorts);

        return sorts.stream()
                .map(sort -> builder.call(SqlStdOperatorTable.NULLS_FIRST, sort))
                .collect(Collectors.toList());
    }

    /**
     * Returns the RexNode used to filter the druidQuery.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The query from which to find filter all the filters for.
     * @param timestampColumn  The name of the timestamp column in the database.
     *
     * @return the combined RexNodes that should be filtered on.
     */
    protected RexNode getAllWhereFilters(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            String timestampColumn
    ) {
        RexNode timeFilter = sqlTimeConverter.buildTimeFilters(
                builder,
                druidQuery.getIntervals(),
                timestampColumn
        );

        if (druidQuery.getFilter() != null) {
            FilterEvaluator filterEvaluator = new FilterEvaluator();
            RexNode druidQueryFilter = filterEvaluator.evaluateFilter(builder, druidQuery.getFilter());
            return builder.and(timeFilter, druidQueryFilter);
        }

        return timeFilter;
    }

    /**
     * Gets the collection of having filters to be applied from the druid query.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The query to find the having filter from.
     * @param aliasMaker  The mapping from api to physical name.
     *
     * @return the collection of equivalent filters for calcite.
     */
    protected Collection<RexNode> getHavingFilter(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper aliasMaker
    ) {
        RexNode filter = null;
        if (druidQuery.getQueryType().equals(DefaultQueryType.GROUP_BY)) {
            Having having = ((GroupByQuery) druidQuery).getHaving();

            if (having != null) {
                HavingEvaluator havingEvaluator = new HavingEvaluator();
                filter = havingEvaluator.evaluateHaving(builder, having, aliasMaker);
            }
        }

        return Collections.singletonList(filter);
    }

    /**
     * Find all druid aggregations and convert them to {@link org.apache.calcite.tools.RelBuilder.AggCall}.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The druid query to get the aggregations of.
     * @param aliasMaker  The mapping from api to physical name.
     *
     * @return the list of aggregations.
     */
    protected List<RelBuilder.AggCall> getAllQueryAggregations(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper aliasMaker
    ) {
        return druidQuery.getAggregations()
                .stream()
                .map(druidSqlAggregationConverter::fromDruidType)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(sqlAggregationBuilder -> sqlAggregationBuilder.build(builder))
                .collect(Collectors.toList());
    }

    /**
     * Collects all the time columns and dimensions to be grouped on.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The query to find grouping columns from.
     * @param timestampColumn  The name of the timestamp column in the database.
     *
     * @return all columns which should be grouped on.
     */
    protected List<RexNode> getAllGroupByColumns(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            String timestampColumn
    ) {
        Stream<RexNode> timeFilters = sqlTimeConverter.buildGroupBy(
                builder,
                druidQuery.getGranularity(),
                timestampColumn
        );

        Stream<RexNode> dimensionFilters = druidQuery.getDimensions().stream()
                .map(Dimension::getApiName)
                .map(builder::field);

        return Stream.concat(timeFilters, dimensionFilters).collect(Collectors.toList());
    }

    /**
     * Converts a RelBuilder into a sql string.
     *
     * @param sqlWriter  The writer to be used when translating the {@link org.apache.calcite.rel.RelNode} to sql.
     * @param relToSql  The converter from {@link org.apache.calcite.rel.RelNode} to
     * {@link org.apache.calcite.sql.SqlNode}.
     * @param builder  The RelBuilder created with Calcite.
     *
     * @return the sql string built by the RelBuilder.
     */
    protected String writeSql(SqlPrettyWriter sqlWriter, RelToSqlConverter relToSql, RelBuilder builder) {
        sqlWriter.reset();
        SqlSelect select = relToSql.visitChild(0, builder.build()).asSelect();
        return sqlWriter.format(select);
    }

    public SqlTimeConverter getTimeConverter() {
        return sqlTimeConverter;
    }
}
