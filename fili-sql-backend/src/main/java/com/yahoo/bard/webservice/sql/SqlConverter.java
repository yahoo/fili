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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

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
        this(dataSource, DEFAULT_SCHEMA);
    }

    /**
     * Creates a sql converter using the given database and datasource.
     *
     * @param dataSource  The dataSource for the jdbc schema.
     * @param schemaName  The name of the schema used for the database.
     *
     * @throws SQLException if can't read from database.
     */
    public SqlConverter(DataSource dataSource, String schemaName) throws SQLException {
        connection = dataSource.getConnection();
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
        return rootSchema.add(
                schemaName,
                JdbcSchema.create(rootSchema, null, dataSource, null, null)
        );
    }

    @Override
    public Future<JsonNode> executeQuery(DruidAggregationQuery<?> druidQuery) {
        LOG.trace("Original Query\n {}", JSON_WRITER.valueToTree(druidQuery));

        DefaultQueryType queryType = (DefaultQueryType) druidQuery.getQueryType();
        LOG.debug("Processing {} query", queryType);

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
        String sqlQuery = "";
        try {
            sqlQuery = buildSqlQuery(connection, druidQuery);
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't generate sql", e);
        }

        LOG.trace("Executing \n{}", sqlQuery);
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
        // DatabaseHelper.print(resultSet);
        int rows = 0;
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        DruidResponse druidResponse = new DruidResponse();
        while (resultSet.next()) { // todo this is different for other queries
            ++rows;

            int groupByCount = druidQuery.getDimensions().size();

            DateTime timestamp = TimeConverter.parseDateTime(groupByCount, resultSet, druidQuery.getGranularity());
            Map<String, String> sqlResults = new HashMap<>();
            DruidResultRow rowResult;
            if (druidQuery.getQueryType().equals(DefaultQueryType.GROUP_BY)) {
                rowResult = new GroupByResultRow(timestamp, GroupByResultRow.Version.V1);
            } else if (druidQuery.getQueryType().equals(DefaultQueryType.TOP_N)) {
                rowResult = new TopNResultRow(timestamp);
            } else {
                rowResult = new TimeseriesResultRow(timestamp);
            }

            for (int i = 1; i <= groupByCount; i++) {
                String columnName = ALIAS_MAKER.unApply(resultSetMetaData.getColumnName(i));
                String val = resultSet.getString(i);
                sqlResults.put(columnName, val);
                rowResult.add(
                        columnName,
                        resultTypeMapper.getOrDefault(columnName, String::toString).apply(val)
                );
            }

            int lastTimeIndex = TimeConverter.getNumberOfGroupByFunctions(druidQuery.getGranularity());
            for (int i = groupByCount + lastTimeIndex + 1; i <= resultSetMetaData.getColumnCount(); i++) {
                String columnName = ALIAS_MAKER.unApply(resultSetMetaData.getColumnName(i));
                String val = resultSet.getString(i);
                sqlResults.put(columnName, val);
                rowResult.add(columnName, resultTypeMapper.get(columnName).apply(val));
            }

            druidQuery.getPostAggregations()
                    .forEach(postAggregation -> {
                        Double postAggResult = PostAggregationEvaluator.evaluate(postAggregation, sqlResults);
                        rowResult.add(postAggregation.getName(), postAggResult);
                    });

            druidResponse.add(rowResult);
        }
        LOG.debug("Fetched {} rows.", rows);

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

        LOG.debug("Selecting SQL Table '{}'", sqlTableName);
        builder.scan(sqlTableName);

        LOG.debug("Selecting all columns of dimensions for filters and aggregations");
        List<RexInputRef> columnsToSelect = getColumnsToSelect(druidQuery, nameOfTimestampColumn);
        builder.project(columnsToSelect);

        if (druidQuery.getQueryType().equals(DefaultQueryType.TOP_N)) {
            return finishTopN((TopNQuery) druidQuery, nameOfTimestampColumn);
        }

        LOG.debug("Adding filters");
        List<RexNode> allFilters = getAllWhereFilters(druidQuery, nameOfTimestampColumn);
        builder.filter(allFilters);

        LOG.debug("Adding aggregations and having filters");
        List<RexNode> groupColumns = addGroupByAggregationsAndHavingClauses(druidQuery, nameOfTimestampColumn);

        LOG.debug("Adding sorts to output");
        // todo check for sorting from druidQuery
        // this is somewhat bad, todo look into getting rexnode references and using them down here
        List<RexNode> sorts = new ArrayList<>();
        sorts.addAll(builder.fields().subList(druidQuery.getDimensions().size(), groupColumns.size()));
        sorts.addAll(builder.fields().subList(0, druidQuery.getDimensions().size()));
        builder.sort(sorts);


        // NOTE: does not include missing interval or meta information
        // this will have to be implemented later if at all since we don't know about partial data

        return relToSql(builder);
    }

    private String finishTopN(TopNQuery topNQuery, String nameOfTimestampColumn) {
        RelNode rootRelNode = builder.build();
        return IntervalUtils.getSlicedIntervals(topNQuery.getIntervals(), topNQuery.getGranularity())
                .keySet()
                .stream()
                .map(interval -> {
                    builder.push(rootRelNode);
                    List<RexNode> filters = getAllWhereFilters(topNQuery, nameOfTimestampColumn);
                    builder.filter(filters);
                    return builder.build();
                })
                .map(bucket -> {
                    builder.push(bucket);
                    addGroupByAggregationsAndHavingClauses(topNQuery, nameOfTimestampColumn);
                    builder.sortLimit(
                            0,
                            (int) topNQuery.getThreshold(),
                            builder.desc(builder.field(ALIAS_MAKER.apply(topNQuery.getMetric().getMetric().toString())))
                            // todo this should be asc/desc based on the query
                    );
                    return builder.build();
                })
                .map(relNode -> {
                    builder.push(relNode);
                    String sql = relToSql(builder);
                    return "(" + sql + ")";
                })
                .collect(Collectors.joining("\nUNION ALL\n"));

        // calcite unions aren't working correctly (syntax error)
        // builder.pushAll(finishedQueries);
        // builder.union(true, finishedQueries.size());
        // so the below solution manually does this part
    }

    /**
     * Returns a list of all the RexNodes used to filter the druidQuery.
     *
     * @param druidQuery  The query from which to find filter all the filters for.
     * @param nameOfTimestampColumn  The name of the timestamp column in the database.
     *
     * @return the list of RexNodes that should be filtered on.
     */
    private List<RexNode> getAllWhereFilters(DruidAggregationQuery<?> druidQuery, String nameOfTimestampColumn) {
        List<RexNode> filters = new ArrayList<>();
        RexNode timeFilter = TimeConverter.buildTimeFilters(builder, druidQuery.getIntervals(), nameOfTimestampColumn);
        filters.add(timeFilter);

        RexNode druidQueryFilter = FilterEvaluator.getFilterAsRexNode(builder, druidQuery.getFilter());
        if (druidQueryFilter != null) {
            filters.add(druidQueryFilter);
        }

        return filters;
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
        List<String> filterDimensions = FilterEvaluator.getDimensionNames(builder, druidQuery.getFilter());

        List<String> groupByDimensions = druidQuery.getDimensions()
                .stream()
                .map(Dimension::getApiName)
                .collect(Collectors.toList());

        List<String> aggregationDimensions = druidQuery.getAggregations()
                .stream()
                .map(Aggregation::getFieldName)
                .collect(Collectors.toList());


        List<String> allFilters = new ArrayList<>(
                filterDimensions.size() +
                        groupByDimensions.size() +
                        aggregationDimensions.size() +
                        1
        );
        allFilters.add(nameOfTimestampColumn);
        allFilters.addAll(filterDimensions);
        allFilters.addAll(groupByDimensions);
        allFilters.addAll(aggregationDimensions);

        return allFilters.stream()
                .map(builder::field)
                .collect(Collectors.toList());
    }

    /**
     * Creates the aggregations, i.e. (SUM,MIN,MAX) in sql from the druidQuery's aggregations
     * and then groups by the time columns corresponding to the granularity.
     *
     * @param druidQuery  The query to build aggregations from.
     * @param nameOfTimestampColumn  The name of the timestamp column in the database.
     *
     * @return the list of fields which are being grouped by.
     */
    private List<RexNode> addGroupByAggregationsAndHavingClauses(
            DruidAggregationQuery<?> druidQuery,
            String nameOfTimestampColumn
    ) {
        List<RelBuilder.AggCall> druidAggregations = getAllQueryAggregations(druidQuery);

        List<RexNode> allGroupBys = getAllGroupByColumns(druidQuery, nameOfTimestampColumn);

        builder.aggregate(
                builder.groupKey(
                        allGroupBys
                ),
                druidAggregations
        );

        if (druidQuery.getQueryType().equals(DefaultQueryType.GROUP_BY)) {
            Having having = ((GroupByQuery) druidQuery).getHaving();
            if (having != null) {
                builder.filter(HavingEvaluator.evaluate(builder, having, ALIAS_MAKER));
            }
        }

        return allGroupBys;
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
        List<RexNode> timeFilters = TimeConverter.buildGroupBy(
                builder,
                druidQuery.getGranularity(),
                nameOfTimestampColumn
        );

        List<RexNode> dimensionFilters = druidQuery.getDimensions().stream()
                .map(Dimension::getApiName)
                .map(builder::field)
                .collect(Collectors.toList());

        List<RexNode> allGroupByColumns = new ArrayList<>(
                timeFilters.size() +
                        dimensionFilters.size()
        );
        allGroupByColumns.addAll(timeFilters);
        allGroupByColumns.addAll(dimensionFilters);

        return allGroupByColumns;
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
    private Map<String, Function<String, Object>> getAggregationTypeMapper(
            DruidAggregationQuery<?> druidQuery
    ) {
        //todo maybe "true"/"false" -> boolean
        return druidQuery.getAggregations()
                .stream()
                .collect(Collectors.toMap(Aggregation::getName, aggregation -> {
                    Function<String, Object> typeMapper;
                    String aggType = aggregation.getType().toLowerCase(Locale.ENGLISH);
                    if (aggType.contains("long")) {
                        typeMapper = Long::parseLong;
                    } else if (aggType.contains("double")) {
                        typeMapper = Double::parseDouble;
                    } else {
                        typeMapper = String::toString;
                    }
                    return typeMapper;
                }));
    }

    /**
     * Converts a RelBuilder into a sql string.
     *
     * @param builder  The RelBuilder created with Calcite.
     *
     * @return the sql string built by the RelBuilder.
     */
    private String relToSql(RelBuilder builder) {
        return relToSql.visitChild(0, builder.build()).asSelect().toString();
    }
}
