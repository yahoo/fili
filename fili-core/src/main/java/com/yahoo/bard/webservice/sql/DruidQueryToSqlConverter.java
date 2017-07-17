// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec;
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection;
import com.yahoo.bard.webservice.druid.model.orderby.TopNMetric;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery;
import com.yahoo.bard.webservice.druid.model.query.TopNQuery;
import com.yahoo.bard.webservice.sql.aggregation.DefaultDruidSqlAggregationConverter;
import com.yahoo.bard.webservice.sql.aggregation.DruidSqlAggregationConverter;
import com.yahoo.bard.webservice.sql.evaluator.FilterEvaluator;
import com.yahoo.bard.webservice.sql.evaluator.HavingEvaluator;
import com.yahoo.bard.webservice.sql.helper.CalciteHelper;
import com.yahoo.bard.webservice.sql.helper.DatabaseHelper;
import com.yahoo.bard.webservice.sql.helper.DefaultSqlTimeConverter;
import com.yahoo.bard.webservice.sql.helper.SqlTimeConverter;
import com.yahoo.bard.webservice.util.IntervalUtils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.tools.RelBuilder;
import org.joda.time.Interval;
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
    private static final int NONE = -1;

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
            case TOP_N:
            case GROUP_BY:
            case TIMESERIES:
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
    public List<String> buildSqlQuery(
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
                .project(
                        getColumnsToSelect(builder, druidQuery, timestampColumn, apiToFieldMapper)
                );
        RelNode rootRelNode = builder.build();
        RelToSqlConverter relToSql = calciteHelper.getNewRelToSqlConverter();
        SqlPrettyWriter sqlWriter = calciteHelper.getNewSqlWriter();
        return getIntervals(druidQuery)
                .map(interval -> {
                    RelBuilder localBuilder = calciteHelper.getNewRelBuilder();

                    localBuilder.push(rootRelNode)
                            .filter(
                                    getAllWhereFilters(
                                            localBuilder,
                                            druidQuery,
                                            timestampColumn,
                                            interval
                                    )
                            )
                            .aggregate(
                                    localBuilder.groupKey(
                                            getAllGroupByColumns(localBuilder, druidQuery, timestampColumn)
                                    ),
                                    getAllQueryAggregations(localBuilder, druidQuery, apiToFieldMapper)
                            )
                            .filter(
                                    getHavingFilter(localBuilder, druidQuery, apiToFieldMapper)
                            )
                            .sortLimit(
                                    NONE,
                                    getThreshold(druidQuery),
                                    getSort(localBuilder, druidQuery, apiToFieldMapper)
                            );
                    return writeSql(sqlWriter, relToSql, localBuilder);
                })
                .collect(Collectors.toList());

        // calcite unions aren't working correctly (syntax error)
        // builder.pushAll(finishedQueries);
        // builder.union(true, finishedQueries.size());

        // NOTE: does not include missing interval or meta information
        // this will have to be implemented later if at all since we don't know about partial data
    }

    /**
     * Gets a stream of a list of intervals to query over. TopN queries are a stream of single item lists
     * because the sort/limiting has to be done on each seperately. Other queries are a single item stream of
     * a list of intervals.
     *
     * @param druidQuery  The query to find the intervals from.
     *
     * @return the collection of interval groups to query over.
     */
    protected static Stream<List<Interval>> getIntervals(DruidAggregationQuery<?> druidQuery) {
        if (druidQuery.getQueryType().equals(DefaultQueryType.TOP_N)) {
            return IntervalUtils.getSlicedIntervals(druidQuery.getIntervals(), druidQuery.getGranularity())
                    .keySet()
                    .stream()
                    .map(Collections::singletonList);
        } else {
            return Stream.of(druidQuery.getIntervals());
        }
    }

    /**
     * Finds the sorting for a druid query.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The query to find the sorting from.
     * @param aliasMaker  The mapping from api to physical names.
     *
     * @return a collection of rexnodes to apply sorts in calcite.
     */
    protected Collection<RexNode> getSort(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper aliasMaker
    ) {
        // druid does NULLS FIRST
        List<RexNode> sorts = new ArrayList<>();
        if (druidQuery.getQueryType().equals(DefaultQueryType.TOP_N)) {
            TopNQuery topNQuery = (TopNQuery) druidQuery;
            Object topNMetricValue = topNQuery.getMetric().getMetric();

            if (topNMetricValue instanceof TopNMetric) { //todo this is ugly but we don't have enough information
                TopNMetric inner = ((TopNMetric) topNMetricValue);
                String metricName = inner.getMetric().toString();
                sorts.add(builder.field(aliasMaker.apply(metricName)));
            } else {
                String metricName = topNMetricValue.toString();
                sorts.add(builder.desc(builder.field(aliasMaker.unApply(metricName))));
            }
            sorts.add(builder.field(topNQuery.getDimension().getApiName()));
        } else {

            int groupBys = druidQuery.getDimensions()
                    .size() + sqlTimeConverter.getNumberOfGroupByFunctions((TimeGrain) druidQuery.getGranularity());
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
                            .forEach(sorts::add);
                }
            }
            sorts.addAll(builder.fields().subList(druidQuery.getDimensions().size(), groupBys));
            sorts.addAll(builder.fields().subList(0, druidQuery.getDimensions().size()));
        }

        return sorts.stream()
                .map(sort -> builder.call(SqlStdOperatorTable.NULLS_FIRST, sort))
                .collect(Collectors.toList());
    }

    /**
     * Finds the limit/threshold of results to return from the query.
     *
     * @param druidQuery  The query to find the threshold for.
     *
     * @return the threshold or {@link #NONE}.
     */
    protected int getThreshold(DruidAggregationQuery<?> druidQuery) {
        if (druidQuery.getQueryType().equals(DefaultQueryType.TOP_N)) {
            return (int) ((TopNQuery) druidQuery).getThreshold();
        } else if (druidQuery.getQueryType().equals(DefaultQueryType.GROUP_BY)) {
            LimitSpec limitSpec = ((GroupByQuery) druidQuery).getLimitSpec();
            if (limitSpec != null && limitSpec.getLimit().isPresent()) {
                return limitSpec.getLimit().getAsInt();
            }
        }
        return NONE;
    }

    /**
     * Returns the RexNode used to filter the druidQuery.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The query from which to find filter all the filters for.
     * @param timestampColumn  The name of the timestamp column in the database.
     * @param intervals  The intervals to select events from.
     *
     * @return the combined RexNodes that should be filtered on.
     */
    protected RexNode getAllWhereFilters(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            String timestampColumn,
            Collection<Interval> intervals
    ) {
        RexNode timeFilter = sqlTimeConverter.buildTimeFilters(builder, intervals, timestampColumn);

        if (druidQuery.getFilter() != null) {
            FilterEvaluator filterEvaluator = new FilterEvaluator();
            RexNode druidQueryFilter = filterEvaluator.evaluateFilter(builder, druidQuery.getFilter());
            return builder.and(timeFilter, druidQueryFilter);
        }

        return timeFilter;
    }

    /**
     * Finds all the dimensions from filters, aggregations, and the time column
     * which need to be selected for the sql query.
     *
     * @param builder  The RelBuilder created with Calcite.
     * @param druidQuery  The query from which to find filter and aggregation dimensions.
     * @param timestampColumn  The name of the timestamp column in the database.
     * @param aliasMaker  The mapping from api to physical names.
     *
     * @return the list of fields which need to be selected by the builder.
     */
    protected List<RexInputRef> getColumnsToSelect(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            String timestampColumn,
            ApiToFieldMapper aliasMaker
    ) {
        FilterEvaluator filterEvaluator = new FilterEvaluator();

        Stream<String> filterDimensions = filterEvaluator.getDimensionNames(builder, druidQuery.getFilter()).stream()
                .map(aliasMaker);

        Stream<String> groupByDimensions = druidQuery.getDimensions()
                .stream()
                .map(Dimension::getApiName)
                .map(aliasMaker); //todo is this fine?

        Stream<String> aggregationDimensions = druidQuery.getAggregations()
                .stream()
                .map(Aggregation::getFieldName); //todo is this fine?

        return Stream
                .concat(
                        Stream.concat(Stream.of(timestampColumn), aggregationDimensions),
                        Stream.concat(filterDimensions, groupByDimensions)
                )
                .map(builder::field)
                .collect(Collectors.toList());
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
                (TimeGrain) druidQuery.getGranularity(),
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
     * @param sqlWriter  The writer to be used when translating the {@link RelNode} to sql.
     * @param relToSql  The converter from {@link RelNode} to {@link org.apache.calcite.sql.SqlNode}.
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
