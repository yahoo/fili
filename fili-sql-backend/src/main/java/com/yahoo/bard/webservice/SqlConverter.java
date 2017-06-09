// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;


import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.query.AbstractDruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery;
import com.yahoo.bard.webservice.druid.response.DruidResponse;
import com.yahoo.bard.webservice.druid.response.TimeseriesResultRow;
import com.yahoo.bard.webservice.util.FailedFuture;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.xml.internal.ws.util.CompletedFuture;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
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
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

/**
 * Converts druid queries to sql, executes it, and returns a druid like response.
 */
public class SqlConverter implements SqlBackedClient {
    public static final String THE_SCHEMA = "DEFAULT_SCHEMA";
    private static final Logger LOG = LoggerFactory.getLogger(SqlConverter.class);
    private static final String ALIAS = "__";
    private static final Function<String, String> ALIAS_MAKER = (fieldName) -> ALIAS + fieldName;
    private static final ObjectMapper JSON_WRITER = new ObjectMapper();
    private final RelBuilder builder;
    private final RelToSqlConverter relToSql;
    private final Connection connection;

    /**
     * Creates a sql converter using the given database and datasource.
     * NOTE: as of right now the table must be using a schema called "DEFAULT_SCHEMA"
     *
     * @param connection  The connection to the database.
     * @param dataSource  The dataSource for the jdbc schema.
     *
     * @throws SQLException if can't read from database.
     */
    public SqlConverter(Connection connection, DataSource dataSource) throws SQLException {
        this.connection = connection;
        relToSql = new RelToSqlConverter(SqlDialect.create(connection.getMetaData()));
        builder = builder(connection, dataSource);
    }

    /**
     * Creates a {@link RelBuilder} with a root scema of {@link #THE_SCHEMA}.
     *
     * @param connection  The connection to the database.
     * @param dataSource  The dataSource for the jdbc schema.
     *
     * @return the relbuilder from Calcite.
     *
     * @throws SQLException if can't read from database.
     */
    private static RelBuilder builder(Connection connection, DataSource dataSource) throws SQLException {
        // todo create schema here or find a way to not have to use it
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        return RelBuilder.create(
                Frameworks.newConfigBuilder()
                        .parserConfig(SqlParser.Config.DEFAULT)
                        .defaultSchema(addSchema(rootSchema, dataSource))
                        .traitDefs((List<RelTraitDef>) null)
                        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
                        .build()
        );
    }

    /**
     * Adds the {@link #THE_SCHEMA} to the rootSchema.
     *
     * @param rootSchema  The calcite schema for the database.
     * @param dataSource  The dataSource for the jdbc schema.
     *
     * @return the schema.
     */
    private static SchemaPlus addSchema(SchemaPlus rootSchema, DataSource dataSource) {
        return rootSchema.add(
                THE_SCHEMA,
                JdbcSchema.create(rootSchema, null, dataSource, null, null)
        );
    }

    @Override
    public Future<JsonNode> executeQuery(DruidQuery<?> druidQuery) {
        LOG.trace("Original Query\n {}", JSON_WRITER.valueToTree(druidQuery));

        DefaultQueryType queryType = (DefaultQueryType) druidQuery.getQueryType();
        LOG.debug("Processing {} query", queryType);

        JsonNode druidResponse = null;
        switch (queryType) {
            case TIMESERIES:
                TimeSeriesQuery timeSeriesQuery = (TimeSeriesQuery) druidQuery;
                druidResponse = executeAndProcessQuery(connection, timeSeriesQuery);
                break;
        }

        if (druidResponse != null) {
            LOG.debug("Fake Druid Response\n {}", druidResponse);
            return new CompletedFuture<>(druidResponse, null);
        } else {
            LOG.warn("Attempted to query unsupported type {}", queryType.toString());
            return new FailedFuture<>(null);
        }
    }

    /**
     * Builds sql for a TimeSeriesQuery, execute it against the database, process
     * the results and return a jsonNode in the format of a druid response.
     *
     * @param connection  The connection to the database.
     * @param druidQuery  The timeseries query to build and process.
     *
     * @return a druid-like response to the query.
     */
    private JsonNode executeAndProcessQuery(Connection connection, TimeSeriesQuery druidQuery) {
        String sqlQuery = "";
        Throwable throwable = null;
        try {
            sqlQuery = buildTimeSeriesQuery(connection, druidQuery);
        } catch (SQLException e) {
            throwable = new RuntimeException("Couldn't generate sql", e);
        }

        if (!sqlQuery.isEmpty()) {
            LOG.debug("Executing \n{}", sqlQuery);
            try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                return read(druidQuery, resultSet);
            } catch (SQLException e) {
                LOG.warn(
                        "Failed to query table {} with {}",
                        druidQuery.getDataSource().getPhysicalTable().getName(),
                        sqlQuery
                );
                throwable = e;
            }
        }

        throw new RuntimeException("Could not finish query", throwable);
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
     * @throws SQLException if results can't be read.
     */
    private JsonNode read(AbstractDruidAggregationQuery<?> druidQuery, ResultSet resultSet) throws SQLException {
        Map<String, Function<String, Object>> resultMapper = getAggregationTypeMapper(druidQuery);

        int rows = 0;
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        DruidResponse<TimeseriesResultRow> druidResponse = new DruidResponse<>();
        while (resultSet.next()) {
            ++rows;

            DateTime timestamp = TimeConverter.parseDateTime(resultSet, druidQuery.getGranularity());
            TimeseriesResultRow rowResult = new TimeseriesResultRow(timestamp);
            Map<String, String> sqlResults = new HashMap<>();

            int lastTimeIndex = TimeConverter.getNumberOfGroupByFunctions(druidQuery.getGranularity());
            for (int i = lastTimeIndex + 1; i <= resultSetMetaData.getColumnCount(); i++) {
                // todo this might be slightly different for group by/ other queries
                String columnName = resultSetMetaData.getColumnName(i).replace(ALIAS, "");
                String val = resultSet.getString(i);
                sqlResults.put(columnName, val);
                rowResult.add(columnName, resultMapper.get(columnName).apply(val));
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
     * Builds the timeseries query as sql and returns it as a string.
     *
     * @param connection  The connection to the database.
     * @param druidQuery  The query to convert to sql.
     *
     * @return the sql equivalent of the query.
     *
     * @throws SQLException if can't connect to database.
     */
    private String buildTimeSeriesQuery(Connection connection, TimeSeriesQuery druidQuery) throws SQLException {
        String sqlTableName = druidQuery.getDataSource().getPhysicalTable().getName();
        String nameOfTimestampColumn = DatabaseHelper.getDateTimeColumn(connection, sqlTableName);

        LOG.debug("Selecting SQL Table {}", sqlTableName);
        builder.scan(sqlTableName);

        LOG.debug("Selecting all columns needed dimensions for filters and aggregations");
        builder.project(getSelectedColumns(druidQuery, nameOfTimestampColumn));

        LOG.debug("Adding filter");
        RexNode druidQueryFilter = FilterEvaluator.getFilterAsRexNode(builder, druidQuery.getFilter());
        RexNode timeFilter = buildTimeFilters(druidQuery, nameOfTimestampColumn);

        if (druidQueryFilter != null) {
            builder.filter(timeFilter, druidQueryFilter);
        } else {
            builder.filter(timeFilter);
        }

        LOG.debug("Adding aggregations { {} }", druidQuery.getAggregations());
        addAggregationsAndGroupBy(druidQuery, nameOfTimestampColumn);

        // todo check for sorting from druidQuery
        //this makes a group by on all the parts from the same time sublist
        // somewhat bad, todo look into getting rexnode references and using them down here
        int timeGranularity = TimeConverter.getNumberOfGroupByFunctions(druidQuery.getGranularity());
        builder.sort(builder.fields().subList(0, timeGranularity));

        // NOTE: does not include missing interval or meta information
        // this will have to be implemented later if at all since we don't know about partial data

        return relToSql(builder);
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
    private List<RexInputRef> getSelectedColumns(
            AbstractDruidAggregationQuery<?> druidQuery,
            String nameOfTimestampColumn
    ) {
        // find dimensions which are needed in filters
        Stream<String> filterDimensions = FilterEvaluator.getDimensionNames(builder, druidQuery.getFilter()).stream();

        // find dimensions which are needed in aggregations
        Stream<String> aggregationDimensions = druidQuery.getAggregations()
                .stream()
                .map(Aggregation::getFieldName);

        // take timestamp column, filter dimensions, and aggregation dimensions
        return Stream.concat(Stream.of(nameOfTimestampColumn), Stream.concat(filterDimensions, aggregationDimensions))
                .map(builder::field)
                .collect(Collectors.toList());
    }

    /**
     * Builds the time filters to only select rows that occur within the intervals of the query.
     * NOTE: you must have one interval to select on.
     *
     * @param druidQuery  The query to get intervals from.
     * @param nameOfTimestampColumn  The name of the timestamp column in the database.
     *
     * @return the RexNode for filtering to only the given intervals.
     */
    private RexNode buildTimeFilters(AbstractDruidAggregationQuery<?> druidQuery, String nameOfTimestampColumn) {
        // create filters to only select results within the given intervals
        List<RexNode> timeFilters = druidQuery.getIntervals().stream().map(interval -> {
            Timestamp start = TimestampUtils.timestampFromMillis(interval.getStartMillis());
            Timestamp end = TimestampUtils.timestampFromMillis(interval.getEndMillis());

            return builder.call(
                    SqlStdOperatorTable.AND,
                    builder.call(
                            SqlStdOperatorTable.GREATER_THAN,
                            builder.field(nameOfTimestampColumn),
                            builder.literal(start.toString())
                    ),
                    builder.call(
                            SqlStdOperatorTable.LESS_THAN,
                            builder.field(nameOfTimestampColumn),
                            builder.literal(end.toString())
                    )
            );
        }).collect(Collectors.toList());

        if (timeFilters.size() > 1) {
            return builder.call(SqlStdOperatorTable.OR, timeFilters);
        } else if (timeFilters.size() == 1) {
            return timeFilters.get(0);
        } else {
            throw new IllegalStateException("Must have at least 1 time filter");
        }
    }

    /**
     * Creates the aggregations, i.e. (SUM,MIN,MAX) in sql from the druidQuery's aggregations
     * and then groups by the time columns corresponding to the granularity.
     *
     * @param druidQuery  The query to build aggregations from.
     * @param nameOfTimestampColumn  The name of the timestamp column in the database.
     */
    private void addAggregationsAndGroupBy(AbstractDruidAggregationQuery<?> druidQuery, String nameOfTimestampColumn) {
        if (druidQuery.getAggregations().size() != 0) {
            List<RelBuilder.AggCall> druidAggregations = druidQuery.getAggregations()
                    .stream()
                    .map(aggregation -> SqlAggregationType.getAggregation(aggregation, builder, ALIAS_MAKER))
                    .collect(Collectors.toList());

            builder.aggregate(
                    builder.groupKey(
                            TimeConverter.buildGroupBy(builder, druidQuery.getGranularity(), nameOfTimestampColumn)
                            // todo groupBy queries will need dimensions here
                    ),
                    druidAggregations
            );
        }
    }

    /**
     * Creates a map from each aggregation name, i.e. ("longSum", "doubleSum"),
     * to a function which will parse to the correct type, i.e. (long, double).
     * The default type to map to is a double.
     *
     * @param druidQuery  The query to make a mapper for.
     *
     * @return the map from aggregation name to {@link Double::parseDouble} {@link Long::parseLong}.
     */
    private Map<String, Function<String, Object>> getAggregationTypeMapper(
            AbstractDruidAggregationQuery<?> druidQuery
    ) {
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
                        typeMapper = Double::parseDouble;
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
