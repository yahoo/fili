// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;


import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
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
import com.yahoo.bard.webservice.sql.evaluator.FilterEvaluator;
import com.yahoo.bard.webservice.sql.evaluator.HavingEvaluator;
import com.yahoo.bard.webservice.sql.helper.CalciteHelper;
import com.yahoo.bard.webservice.sql.helper.DatabaseHelper;
import com.yahoo.bard.webservice.sql.helper.SqlAggregationType;
import com.yahoo.bard.webservice.sql.helper.TimeConverter;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.util.CompletedFuture;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

/**
 * Converts druid queries to sql, executes it, and returns a druid like response.
 */
public class SqlConverter implements SqlBackedClient {
    private static final Logger LOG = LoggerFactory.getLogger(SqlConverter.class);
    private final ObjectMapper jsonWriter;
    private final CalciteHelper calciteHelper;
    private final PhysicalTableDictionary physicalTableDictionary;

    /**
     * Creates a sql converter using the given database and datasource.
     * The default schema is "PUBLIC" (i.e. you haven't called "create schema"
     * and "set schema")
     *
     * @param dataSource  The dataSource for the jdbc schema.
     *
     * @throws SQLException if can't read from database.
     */
    public SqlConverter(
            PhysicalTableDictionary physicalTableDictionary,
            DataSource dataSource,
            ObjectMapper objectMapper
    ) throws SQLException {
        calciteHelper = new CalciteHelper(dataSource, CalciteHelper.DEFAULT_SCHEMA);
        jsonWriter = objectMapper;
        this.physicalTableDictionary = physicalTableDictionary;
    }

    /**
     * Creates a sql converter using the given database and datasource.
     *
     * @param physicalTableDictionary
     * @param schemaName  The name of the schema used for the database.
     *
     * @throws SQLException if can't read from database.
     */
    public SqlConverter(
            PhysicalTableDictionary physicalTableDictionary,
            ObjectMapper objectMapper,
            String url,
            String driver,
            String username,
            String password,
            String schemaName
    ) throws SQLException {
        DataSource dataSource = JdbcSchema.dataSource(url, driver, username, password);

        calciteHelper = new CalciteHelper(dataSource, schemaName);
        jsonWriter = objectMapper;
        this.physicalTableDictionary = physicalTableDictionary;
    }

    @Override
    public Future<JsonNode> executeQuery(
            DruidQuery<?> druidQuery,
            SuccessCallback successCallback,
            FailureCallback failureCallback
    ) {
        DefaultQueryType queryType = (DefaultQueryType) druidQuery.getQueryType();
        LOG.debug("Processing {} query\n {}", queryType, jsonWriter.valueToTree(druidQuery));

        switch (queryType) {
            case TOP_N:
            case GROUP_BY:
            case TIMESERIES:
                CompletableFuture<JsonNode> responseFuture = CompletableFuture.supplyAsync(() -> {
                            try {
                                return executeAndProcessQuery((DruidAggregationQuery) druidQuery);
                            } catch (RuntimeException e) {
                                LOG.warn("Failed while querying ", e);
                                if (failureCallback != null) {
                                    failureCallback.dispatch(e);
                                }
                            }
                            return null;
                        }
                );
                responseFuture.thenAccept(jsonNode -> {
                    if (jsonNode != null && successCallback != null) {
                        successCallback.invoke(jsonNode);
                    }
                });
                return responseFuture;
            default:
                String message = "Unable to process " + queryType.toString();
                failureCallback.invoke(new UnsupportedOperationException(message));
        }
        return new CompletedFuture<>(null, null);
    }


    /**
     * Builds sql for a druid query, execute it against the database, process
     * the results and return a jsonNode in the format of a druid response.
     *
     * @param druidQuery  The druid query to build and process.
     *
     * @return a druid-like response to the query.
     */
    private JsonNode executeAndProcessQuery(
            DruidAggregationQuery<?> druidQuery
    ) {
        String sqlQuery;

        ApiToFieldMapper aliasMaker = new ApiToFieldMapper(physicalTableDictionary.get(getTableName(druidQuery))
                .getSchema());

        try (Connection connection = calciteHelper.getConnection()) {
            sqlQuery = buildSqlQuery(connection, druidQuery, aliasMaker);
            LOG.info("Executing \n{}", sqlQuery);

            SqlResultSetProcessor resultSetProcessor;
            try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                resultSetProcessor = readSqlResultSet(druidQuery, resultSet, aliasMaker);
            } catch (SQLException e) {
                LOG.warn(
                        "Failed to query table {}",
                        druidQuery.getDataSource().getPhysicalTable().getName()
                );
                throw new RuntimeException("Could not finish query", e);
            }

            JsonNode jsonNode = resultSetProcessor.process();
            LOG.debug("Created response: {}", jsonNode);
            return jsonNode;
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't generate sql", e);
        }


    }

    /**
     * Reads the result set and converts it into a result that druid
     * would produce.
     *
     * @param druidQuery the druid query to be made.
     * @param resultSet  the result set of the druid query.
     *
     * @param aliasMaker
     * @return druid-like result from query.
     *
     * @throws SQLException if results can't be readSqlResultSet.
     */
    private SqlResultSetProcessor readSqlResultSet(
            DruidAggregationQuery<?> druidQuery,
            ResultSet resultSet,
            final ApiToFieldMapper aliasMaker
    )
            throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();

        BiMap<Integer, String> columnNames = HashBiMap.create(columnCount);
        List<String[]> sqlResults = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = aliasMaker.unApply(resultSetMetaData.getColumnName(i));
            columnNames.put(i - 1, columnName);
        }

        while (resultSet.next()) {
            String[] row = new String[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                row[i - 1] = resultSet.getString(i);
            }
            sqlResults.add(row);
        }
        LOG.debug("Fetched {} rows.", sqlResults.size());

        return new SqlResultSetProcessor(druidQuery, columnNames, sqlResults, jsonWriter, aliasMaker);
    }

    /**
     * Builds the druid query as sql and returns it as a string.
     *
     * @param connection  The connection to the database.
     * @param druidQuery  The query to convert to sql.
     *
     * @return the sql equivalent of the query.
     *
     * @throws SQLException if can't connect to database.
     */
    private String buildSqlQuery(
            Connection connection,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper aliasMaker
    )
            throws SQLException {
        String sqlTableName = druidQuery.getDataSource().getPhysicalTable().getName();
        String timestampColumn = DatabaseHelper.getTimestampColumn(
                connection,
                calciteHelper.escapeTableName(sqlTableName)
        );

        RelBuilder builder = calciteHelper.getNewRelBuilder();
        LOG.debug("Querying Table '{}'", sqlTableName);
        builder.scan(sqlTableName)
                .project(
                        getColumnsToSelect(builder, druidQuery, timestampColumn)
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
                                    getAllQueryAggregations(localBuilder, druidQuery, aliasMaker)
                            )
                            .filter(
                                    getHavingFilter(localBuilder, druidQuery, aliasMaker)
                            )
                            .sortLimit(
                                    -1,
                                    getThreshold(druidQuery),
                                    getSort(localBuilder, druidQuery, aliasMaker)
                            );
                    String sql = writeSql(sqlWriter, relToSql, localBuilder);
                    return "(" + sql + ")";
                })
                .collect(Collectors.joining("\nUNION ALL\n"));

        // calcite unions aren't working correctly (syntax error)
        // builder.pushAll(finishedQueries);
        // builder.union(true, finishedQueries.size());

        // NOTE: does not include missing interval or meta information
        // this will have to be implemented later if at all since we don't know about partial data
    }

    private static String getTableName(DruidAggregationQuery<?> druidQuery) {
        return druidQuery.getDataSource().getPhysicalTable().getName();
    }

    private int getFetchSize(final DataApiRequest request) {
        Optional<PaginationParameters> paginationParameters = request.getPaginationParameters();
        if (paginationParameters.isPresent()) {
            PaginationParameters parameters = paginationParameters.get();
            return parameters.getPerPage();
        }
        return -1;
    }

    private int getOffset(DataApiRequest dataApiRequest) {
        Optional<PaginationParameters> paginationParameters = dataApiRequest.getPaginationParameters();
        if (paginationParameters.isPresent()) {
            PaginationParameters parameters = paginationParameters.get();
            return (parameters.getPage() - 1) * parameters.getPerPage();
        }
        //offset = page*perPage
        return -1;
    }

    private static Stream<List<Interval>> getIntervals(DruidAggregationQuery<?> druidQuery) {
        if (druidQuery.getQueryType().equals(DefaultQueryType.TOP_N)) {
            return IntervalUtils.getSlicedIntervals(druidQuery.getIntervals(), druidQuery.getGranularity())
                    .keySet()
                    .stream()
                    .map(Collections::singletonList);
        } else {
            return Collections.singletonList(druidQuery.getIntervals()).stream();
        }
    }

    private static Collection<RexNode> getSort(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            final ApiToFieldMapper aliasMaker
    ) {
        // todo this should be asc/desc based on the query
        // druid does NULLS FIRST
        List<RexNode> sorts = new ArrayList<>();
        if (druidQuery.getQueryType().equals(DefaultQueryType.TOP_N)) {
            TopNQuery topNQuery = (TopNQuery) druidQuery;
            Object topNMetricValue = topNQuery.getMetric().getMetric();

            if (topNMetricValue instanceof TopNMetric) { //todo this is ugly but the it's the interface we're given
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
                    .size() + TimeConverter.getNumberOfGroupByFunctions(druidQuery.getGranularity());
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

    private static int getThreshold(DruidAggregationQuery<?> druidQuery) {
        int noLimit = -1;
        if (druidQuery.getQueryType().equals(DefaultQueryType.TOP_N)) {
            return (int) ((TopNQuery) druidQuery).getThreshold();
        } else if (druidQuery.getQueryType().equals(DefaultQueryType.GROUP_BY)) {
            LimitSpec limitSpec = ((GroupByQuery) druidQuery).getLimitSpec();
            if (limitSpec != null && limitSpec.getLimit().isPresent()) {
                return limitSpec.getLimit().getAsInt();
            }
        }
        return noLimit;
    }

    /**
     * Returns the RexNode used to filter the druidQuery.
     *
     * @param druidQuery  The query from which to find filter all the filters for.
     * @param timestampColumn  The name of the timestamp column in the database.
     * @param intervals  The intervals to select events from.
     *
     * @return the combined RexNodes that should be filtered on.
     */
    private RexNode getAllWhereFilters(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            String timestampColumn,
            Collection<Interval> intervals
    ) {
        RexNode timeFilter = TimeConverter.buildTimeFilters(builder, intervals, timestampColumn);
        Optional<RexNode> druidQueryFilter = FilterEvaluator.getFilterAsRexNode(builder, druidQuery.getFilter());

        if (druidQueryFilter.isPresent()) {
            return builder.and(timeFilter, druidQueryFilter.get());
        } else {
            return timeFilter;
        }
    }

    /**
     * Finds all the dimensions from filters, aggregations, and the time column
     * which need to be selected for the sql query.
     *
     * @param druidQuery  The query from which to find filter and aggregation dimensions.
     * @param timestampColumn  The name of the timestamp column in the database.
     *
     * @return the list of fields which need to be selected by the builder.
     */
    private static List<RexInputRef> getColumnsToSelect(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            String timestampColumn
    ) {
        Stream<String> filterDimensions = FilterEvaluator.getDimensionNames(builder, druidQuery.getFilter()).stream();

        Stream<String> groupByDimensions = druidQuery.getDimensions()
                .stream()
                .map(Dimension::getApiName);

        Stream<String> aggregationDimensions = druidQuery.getAggregations()
                .stream()
                .map(Aggregation::getFieldName);

        return Stream
                .concat(
                        Stream.concat(Stream.of(timestampColumn), aggregationDimensions),
                        Stream.concat(filterDimensions, groupByDimensions)
                )
                .map(builder::field)
                .collect(Collectors.toList());
    }

    /**
     * Creates the aggregations, i.e. (SUM,MIN,MAX) in sql from the druidQuery's aggregations
     * and then groups by the time columns corresponding to the granularity.
     *
     * @return the list of fields which are being grouped by.
     */
    private void addGroupByAggregationsAndHavingClauses() {

    }

    private static Collection<RexNode> getHavingFilter(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper aliasMaker
    ) {
        RexNode filter = null;
        if (druidQuery.getQueryType().equals(DefaultQueryType.GROUP_BY)) {
            Having having = ((GroupByQuery) druidQuery).getHaving();
            filter = HavingEvaluator.buildFilter(builder, having, aliasMaker).orElse(null);
        }
        return Collections.singletonList(filter);
    }

    /**
     * Find all druid aggregations and convert them to {@link org.apache.calcite.tools.RelBuilder.AggCall}.
     *
     * @param druidQuery  The druid query to get the aggregations of.
     *
     * @return the list of aggregations.
     */
    private static List<RelBuilder.AggCall> getAllQueryAggregations(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            ApiToFieldMapper aliasMaker
    ) {
        return druidQuery.getAggregations()
                .stream()
                .map(aggregation -> SqlAggregationType.getAggregation(aggregation, builder))
                .collect(Collectors.toList());
    }

    /**
     * Collects all the time columns and dimensions to be grouped on.
     *
     * @param druidQuery  The query to find grouping columns from.
     * @param timestampColumn  The name of the timestamp column in the database.
     *
     * @return all columns which should be grouped on.
     */
    private static List<RexNode> getAllGroupByColumns(
            RelBuilder builder,
            DruidAggregationQuery<?> druidQuery,
            String timestampColumn
    ) {
        Stream<RexNode> timeFilters = TimeConverter.buildGroupBy(
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
     *
     * @param sqlWriter
     * @param relToSql
     *@param builder  The RelBuilder created with Calcite.
     *  @return the sql string built by the RelBuilder.
     */
    private String writeSql(
            final SqlPrettyWriter sqlWriter,
            final RelToSqlConverter relToSql,
            RelBuilder builder
    ) {
        sqlWriter.reset();
        SqlSelect select = relToSql.visitChild(0, builder.build()).asSelect();
        return sqlWriter.format(select);
    }
}
