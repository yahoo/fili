// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;


import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery;
import com.yahoo.bard.webservice.druid.model.query.TopNQuery;
import com.yahoo.bard.webservice.druid.response.DruidResponse;
import com.yahoo.bard.webservice.druid.response.DruidResultRow;
import com.yahoo.bard.webservice.druid.response.GroupByResultRow;
import com.yahoo.bard.webservice.druid.response.TimeseriesResultRow;
import com.yahoo.bard.webservice.druid.response.TopNResultRow;
import com.yahoo.bard.webservice.util.CompletedFuture;
import com.yahoo.bard.webservice.util.IntervalUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.joda.time.DateTime;
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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

/**
 * Converts druid queries to sql, executes it, and returns a druid like response.
 */
public class SqlConverter implements SqlBackedClient {
    public static final String DEFAULT_SCHEMA = "PUBLIC";
    private static final Logger LOG = LoggerFactory.getLogger(SqlConverter.class);
    private static final String PREPENDED_ALIAS = "__";
    private static final AliasMaker ALIAS_MAKER = new AliasMaker(PREPENDED_ALIAS);
    private static final ObjectMapper JSON_WRITER = new ObjectMapper();
    private final RelBuilder builder;
    private final RelToSqlConverter relToSql;
    private final Connection connection;


    /**
     * Creates a sql converter using the given database and datasource.
     * The default schema is "PUBLIC" (i.e. you haven't called "create schema"
     * and "set schema")
     *
     * @param dataSource  The dataSource for the jdbc schema.
     *
     * @throws SQLException if can't read from database.
     */
    public SqlConverter(DataSource dataSource) throws SQLException {
        connection = dataSource.getConnection();
        relToSql = new RelToSqlConverter(SqlDialect.create(connection.getMetaData()));
        builder = getBuilder(dataSource, DEFAULT_SCHEMA);
    }

    /**
     * Creates a sql converter using the given database and datasource.
     *
     * @param dataSource  The dataSource for the jdbc schema.
     * @param schemaName  The name of the schema used for the database.
     *
     * @throws SQLException if can't read from database.
     */
    public SqlConverter(DataSource dataSource, String username, String password, String schemaName)
            throws SQLException {
        connection = dataSource.getConnection(username, password);
        relToSql = new RelToSqlConverter(SqlDialect.create(connection.getMetaData()));
        builder = getBuilder(dataSource, schemaName);
    }

    /**
     * Creates a {@link RelBuilder} with a root scema of {@link #DEFAULT_SCHEMA}.
     *
     * @param dataSource  The dataSource for the jdbc schema.
     * @param schemaName  The name of the schema used for the database.
     *
     * @return the relbuilder from Calcite.
     *
     * @throws SQLException if can't readSqlResultSet from database.
     */
    private static RelBuilder getBuilder(DataSource dataSource, String schemaName) throws SQLException {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        return RelBuilder.create(
                Frameworks.newConfigBuilder()
                        .parserConfig(SqlParser.Config.DEFAULT)
                        .defaultSchema(addSchema(rootSchema, dataSource, schemaName))
                        .traitDefs((List<RelTraitDef>) null)
                        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
                        .build()
        );
    }

    /**
     * Adds the schema name to the rootSchema.
     *
     * @param rootSchema  The calcite schema for the database.
     * @param dataSource  The dataSource for the jdbc schema.
     * @param schemaName  The name of the schema used for the database.
     *
     * @return the schema.
     */
    private static SchemaPlus addSchema(SchemaPlus rootSchema, DataSource dataSource, String schemaName) {
        // todo look into cloning schema
        return rootSchema.add(
                schemaName,
                JdbcSchema.create(rootSchema, null, dataSource, null, null)
        );
    }

    @Override
    public Future<JsonNode> executeQuery(DruidAggregationQuery<?> druidQuery) {
        DefaultQueryType queryType = (DefaultQueryType) druidQuery.getQueryType();
        LOG.debug("Processing {} query\n {}", queryType, JSON_WRITER.valueToTree(druidQuery));

        JsonNode druidResponse = null;
        try {
            switch (queryType) {
                case TOP_N:
                case GROUP_BY:
                case TIMESERIES:
                    druidResponse = executeAndProcessQuery(connection, druidQuery);
                    break;
            }

            if (druidResponse != null) {
                LOG.debug("Fake Druid Response\n {}", druidResponse);
                return new CompletedFuture<>(druidResponse, null);
            } else {
                String message = "Unable to process " + queryType.toString();
                LOG.warn(message);
                return new CompletedFuture<>(null, new UnsupportedOperationException(message));
            }
        } catch (RuntimeException e) {
            LOG.warn("Failed while querying ", e);
            return new CompletedFuture<>(null, e);
        }
    }

    /**
     * Builds sql for a TimeSeriesQuery, execute it against the database, process
     * the results and return a jsonNode in the format of a druid response.
     *
     * @param connection  The connection to the database.
     * @param druidQuery  The druid query to build and process.
     *
     * @return a druid-like response to the query.
     */
    private JsonNode executeAndProcessQuery(Connection connection, DruidAggregationQuery druidQuery) {
        String sqlQuery;
        try {
            sqlQuery = buildSqlQuery(connection, druidQuery);
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't generate sql", e);
        }

        LOG.debug("Executing \n{}", sqlQuery);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            return readSqlResultSet(druidQuery, resultSet);
        } catch (SQLException e) {
            LOG.warn(
                    "Failed to query table {}",
                    druidQuery.getDataSource().getPhysicalTable().getName()
            );
            throw new RuntimeException("Could not finish query", e);
        }
    }

    /**
     * Reads the result set and converts it into a result that druid
     * would produce.
     *
     * @param druidQuery the druid query to be made.
     * @param resultSet  the result set of the druid query.
     *
     * @return druid-like result from query.
     *
     * @throws SQLException if results can't be readSqlResultSet.
     */
    private JsonNode readSqlResultSet(DruidAggregationQuery<?> druidQuery, ResultSet resultSet) throws SQLException {
        Map<String, Function<String, Object>> resultTypeMapper = getAggregationTypeMapper(druidQuery);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();

        BiMap<Integer, String> columnNames = HashBiMap.create(columnCount);
        List<List<String>> sqlResults = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = ALIAS_MAKER.unApply(resultSetMetaData.getColumnName(i));
            columnNames.put(i, columnName);
        }

        int rows = 0;
        while (resultSet.next()) { // todo this is different for other queries
            sqlResults.add(new ArrayList<>());
            for (int i = 1; i <= columnCount; i++) {
                String val = resultSet.getString(i);
                sqlResults.get(rows).add(val);
            }
            ++rows;
        }
        LOG.debug("Fetched {} rows.", rows);

        DruidResponse druidResponse = new DruidResponse();
        int groupByCount = druidQuery.getDimensions().size();
        sqlResults.stream()
                .map(row -> {
                    DateTime timestamp = TimeConverter.parseDateTime(
                            groupByCount,
                            row,
                            druidQuery.getGranularity()
                    );

                    DruidResultRow rowResult;
                    if (druidQuery.getQueryType().equals(DefaultQueryType.GROUP_BY)) {
                        rowResult = new GroupByResultRow(timestamp, GroupByResultRow.Version.V1);
                    } else if (druidQuery.getQueryType().equals(DefaultQueryType.TOP_N)) {
                        rowResult = new TopNResultRow(timestamp);
                    } else {
                        rowResult = new TimeseriesResultRow(timestamp);
                    }

                    for (int i = 0; i < groupByCount; i++) {
                        Object result = resultTypeMapper
                                .getOrDefault(columnNames.get(i + 1), String::toString)
                                .apply(row.get(i));
                        rowResult.add(columnNames.get(i + 1), result);
                    }

                    int lastTimeIndex = TimeConverter.getNumberOfGroupByFunctions(druidQuery.getGranularity());
                    for (int i = groupByCount + lastTimeIndex; i < columnCount; i++) {
                        Object result = resultTypeMapper
                                .getOrDefault(columnNames.get(i + 1), String::toString)
                                .apply(row.get(i));
                        rowResult.add(columnNames.get(i + 1), result);
                    }

                    druidQuery.getPostAggregations()
                            .forEach(postAggregation -> {
                                Double postAggResult = PostAggregationEvaluator.evaluate(
                                        postAggregation,
                                        (s) -> row.get(columnNames.inverse().get(s))
                                );
                                rowResult.add(postAggregation.getName(), postAggResult);
                            });

                    return rowResult;
                })
                .forEach(druidResponse::add);

        return JSON_WRITER.valueToTree(druidResponse);
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
    private String buildSqlQuery(Connection connection, DruidAggregationQuery<?> druidQuery)
            throws SQLException {
        String sqlTableName = druidQuery.getDataSource().getPhysicalTable().getName();
        // todo we could store the name of the timestamp column
        String nameOfTimestampColumn = DatabaseHelper.getTimestampColumn(connection, sqlTableName);
        if (druidQuery.getQueryType().equals(DefaultQueryType.TOP_N)) {
            return buildTopNSqlQuery((TopNQuery) druidQuery, nameOfTimestampColumn, sqlTableName);
        }

        LOG.debug("Querying Table '{}'", sqlTableName);
        builder.scan(sqlTableName)
                .project(
                        getColumnsToSelect(druidQuery, nameOfTimestampColumn)
                )
                .filter(
                        getAllWhereFilters(druidQuery, nameOfTimestampColumn, druidQuery.getIntervals())
                )
                .aggregate(
                        builder.groupKey(
                                getAllGroupByColumns(druidQuery, nameOfTimestampColumn)
                        ),
                        getAllQueryAggregations(druidQuery)
                )
                .filter(
                        getHavingFilter(druidQuery)
                )
                .sortLimit(
                        -1,
                        getThreshold(druidQuery),
                        getSort(druidQuery)
                );


        // NOTE: does not include missing interval or meta information
        // this will have to be implemented later if at all since we don't know about partial data

        return writeSql(relToSql, builder);
    }

    private Collection<RexNode> getSort(DruidAggregationQuery<?> druidQuery) {
        // todo this should be asc/desc based on the query
        if (druidQuery.getQueryType().equals(DefaultQueryType.TOP_N)) {
            TopNQuery topNQuery = (TopNQuery) druidQuery;
            return Collections.singletonList(
                    builder.desc(builder.field(ALIAS_MAKER.apply(topNQuery.getMetric().getMetric().toString())))
            );
        } else {
            List<RexNode> sorts = new ArrayList<>();
            int groupBys = druidQuery.getDimensions()
                    .size() + TimeConverter.getNumberOfGroupByFunctions(druidQuery.getGranularity());
            sorts.addAll(builder.fields().subList(druidQuery.getDimensions().size(), groupBys));
            sorts.addAll(builder.fields().subList(0, druidQuery.getDimensions().size()));
            return sorts;
        }
    }

    /**
     * Finishes the processing of a TopNQuery after a table and columns have been selected.
     *
     * @param topNQuery  The query to finish processing.
     * @param nameOfTimestampColumn  The name of the timestamp column in the database.
     * @param sqlTableName
     *
     * @return the sql to execute
     */
    private String buildTopNSqlQuery(TopNQuery topNQuery, String nameOfTimestampColumn, String sqlTableName) {
        builder.scan(sqlTableName);
        builder.project(
                getColumnsToSelect(topNQuery, nameOfTimestampColumn)
        );

        RelNode rootRelNode = builder.build();
        return IntervalUtils.getSlicedIntervals(topNQuery.getIntervals(), topNQuery.getGranularity())
                .keySet()
                .stream()
                .map(interval -> {
                    builder.push(rootRelNode)
                            .filter(
                                    getAllWhereFilters(
                                            topNQuery,
                                            nameOfTimestampColumn,
                                            Collections.singletonList(interval)
                                    )
                            )
                            .aggregate(
                                    builder.groupKey(
                                            getAllGroupByColumns(topNQuery, nameOfTimestampColumn)
                                    ),
                                    getAllQueryAggregations(topNQuery)
                            )
                            .sortLimit(
                                    -1,
                                    getThreshold(topNQuery),
                                    getSort(topNQuery)
                            );
                    String sql = writeSql(relToSql, builder);
                    return "(" + sql + ")";
                })
                .collect(Collectors.joining("\nUNION ALL\n"));

        // calcite unions aren't working correctly (syntax error)
        // builder.pushAll(finishedQueries);
        // builder.union(true, finishedQueries.size());
    }

    private static int getThreshold(DruidAggregationQuery<?> druidQuery) {
        if (druidQuery.getQueryType().equals(DefaultQueryType.TOP_N)) {
            return (int) ((TopNQuery) druidQuery).getThreshold();
        }
        return -1;
    }

    /**
     * Returns the RexNode used to filter the druidQuery.
     *
     * @param druidQuery  The query from which to find filter all the filters for.
     * @param nameOfTimestampColumn  The name of the timestamp column in the database.
     * @param intervals  The intervals to select events from.
     *
     * @return the combined RexNodes that should be filtered on.
     */
    private RexNode getAllWhereFilters(
            DruidAggregationQuery<?> druidQuery,
            String nameOfTimestampColumn,
            Collection<Interval> intervals
    ) {
        RexNode timeFilter = TimeConverter.buildTimeFilters(builder, intervals, nameOfTimestampColumn);
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
     * @param nameOfTimestampColumn  The name of the timestamp column in the database.
     *
     * @return the list of fields which need to be selected by the builder.
     */
    private List<RexInputRef> getColumnsToSelect(
            DruidAggregationQuery<?> druidQuery,
            String nameOfTimestampColumn
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
                        Stream.concat(Stream.of(nameOfTimestampColumn), aggregationDimensions),
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
    private List<RexNode> addGroupByAggregationsAndHavingClauses() {
        return null;
    }

    private Collection<RexNode> getHavingFilter(DruidAggregationQuery<?> druidQuery) {
        RexNode filter = null;
        if (druidQuery.getQueryType().equals(DefaultQueryType.GROUP_BY)) {
            Having having = ((GroupByQuery) druidQuery).getHaving();
            filter = HavingEvaluator.buildFilter(builder, having, ALIAS_MAKER).orElse(null);
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
    private List<RelBuilder.AggCall> getAllQueryAggregations(DruidAggregationQuery<?> druidQuery) {
        return druidQuery.getAggregations()
                .stream()
                .map(aggregation -> SqlAggregationType.getAggregation(aggregation, builder, ALIAS_MAKER))
                .collect(Collectors.toList());
    }

    /**
     * Collects all the time columns and dimensions to be grouped on.
     *
     * @param druidQuery  The query to find grouping columns from.
     * @param nameOfTimestampColumn  The name of the timestamp column in the database.
     *
     * @return all columns which should be grouped on.
     */
    private List<RexNode> getAllGroupByColumns(
            DruidAggregationQuery<?> druidQuery,
            String nameOfTimestampColumn
    ) {
        Stream<RexNode> timeFilters = TimeConverter.buildGroupBy(
                builder,
                druidQuery.getGranularity(),
                nameOfTimestampColumn
        );

        Stream<RexNode> dimensionFilters = druidQuery.getDimensions().stream()
                .map(Dimension::getApiName)
                .map(builder::field);

        return Stream.concat(timeFilters, dimensionFilters).collect(Collectors.toList());
    }

    /**
     * Creates a map from each aggregation name, i.e. ("longSum", "doubleSum"),
     * to a function which will parse to the correct type, i.e. (long, double).
     * If no type is found it will do nothing.
     *
     * @param druidQuery  The query to make a mapper for.
     *
     * @return the map from aggregation name to {@link Double::parseDouble} {@link Long::parseLong}.
     */
    private static Map<String, Function<String, Object>> getAggregationTypeMapper(
            DruidAggregationQuery<?> druidQuery
    ) {
        //todo maybe "true"/"false" -> boolean
        return druidQuery.getAggregations()
                .stream()
                .collect(Collectors.toMap(Aggregation::getName, aggregation -> {
                    String aggType = aggregation.getType().toLowerCase(Locale.ENGLISH);
                    if (aggType.contains("long")) {
                        return Long::parseLong;
                    } else if (aggType.contains("double")) {
                        return Double::parseDouble;
                    }
                    return String::toString;
                }));
    }

    /**
     * Converts a RelBuilder into a sql string.
     *
     * @param relToSql
     * @param builder  The RelBuilder created with Calcite.
     *
     * @return the sql string built by the RelBuilder.
     */
    private static String writeSql(RelToSqlConverter relToSql, RelBuilder builder) {
        return relToSql.visitChild(0, builder.build()).asSelect().toString();
    }
}
