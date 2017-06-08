// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;


import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.query.AbstractDruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery;
import com.yahoo.bard.webservice.druid.response.DruidResponse;
import com.yahoo.bard.webservice.druid.response.TimeseriesResult;
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
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

/**
 * Converts druid queries to sql, executes it, and returns a druid like response.
 */
public class SQLConverter implements SqlBackedClient {
    public static final String THE_SCHEMA = "DEFAULT_SCHEMA";
    private static final Logger LOG = LoggerFactory.getLogger(SQLConverter.class);
    private static final String ALIAS = "__";
    private static final ObjectMapper JSON_WRITER = new ObjectMapper();
    private final RelBuilder builder;
    private final RelToSqlConverter relToSql;
    private final Connection connection;

    public SQLConverter(Connection connection, DataSource dataSource) throws SQLException {
        this.connection = connection;
        relToSql = new RelToSqlConverter(SqlDialect.create(connection.getMetaData()));
        builder = builder(connection, dataSource);
    }

    private static RelBuilder builder(Connection connection, DataSource dataSource) throws SQLException {
        // todo create schema here or find a way to not have to use it
        // as of right now the table must be using a schema called "DEFAULT_SCHEMA"
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

    private static SchemaPlus addSchema(SchemaPlus rootSchema, DataSource dataSource) {
        return rootSchema.add(
                THE_SCHEMA,
                JdbcSchema.create(rootSchema, null, dataSource, null, null)
        );
    }

    @Override
    public Future<JsonNode> executeQuery(DruidQuery<?> druidQuery) {
        LOG.debug("Original Query\n {}", JSON_WRITER.valueToTree(druidQuery));

        DefaultQueryType queryType = (DefaultQueryType) druidQuery.getQueryType();
        JsonNode druidResponse = null;
        LOG.debug("Processing {} query", queryType);
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
     */
    private JsonNode read(AbstractDruidAggregationQuery<?> druidQuery, ResultSet resultSet) throws SQLException {
        Map<String, Function<String, Object>> resultMapper = getAggregationTypeMapper(druidQuery);

        int rows = 0;
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        DruidResponse<TimeseriesResult> druidResponse = new DruidResponse<>();
        while (resultSet.next()) {
            ++rows;

            DateTime timestamp = TimeConverter.parseDateTime(resultSet, druidQuery.getGranularity());
            TimeseriesResult rowResult = new TimeseriesResult(timestamp);
            Map<String, String> sqlResults = new HashMap<>();

            int lastTimeIndex = TimeConverter.getDatePartFunctions(druidQuery.getGranularity()).size();
            for (int i = lastTimeIndex + 1; i <= resultSetMetaData.getColumnCount(); i++) {
                // todo this might be slightly different for group by
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

    private String buildTimeSeriesQuery(Connection connection, TimeSeriesQuery druidQuery) throws SQLException {
        String sqlTableName = druidQuery.getDataSource().getPhysicalTable().getName();
        String nameOfTimestampColumn = DatabaseHelper.getDateTimeColumn(connection, sqlTableName).toUpperCase();

        LOG.debug("Selecting SQL Table {}", sqlTableName);
        builder.scan(sqlTableName);

        LOG.debug("Selecting all columns needed dimensions for filters and aggregations");
        builder.project(getGroupByForAllDimensions(druidQuery, nameOfTimestampColumn));

        LOG.debug("Adding filter");
        RexNode druidQueryFilter = FilterEvaluator.getfilterAsRexNode(builder, druidQuery.getFilter());
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
        int timeGranularity = TimeConverter.getDatePartFunctions(druidQuery.getGranularity()).size();
        builder.sort(builder.fields().subList(0, timeGranularity));

        // NOTE: does not include missing interval or meta information
        // this will have to be implemented later if at all since we don't know about partial data

        return relToSql(builder);
    }

    private List<RexInputRef> getGroupByForAllDimensions(
            TimeSeriesQuery druidQuery,
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

    private void addAggregationsAndGroupBy(AbstractDruidAggregationQuery<?> druidQuery, String nameOfTimestampColumn) {
        if (druidQuery.getAggregations().size() != 0) {
            List<RelBuilder.AggCall> druidAggregations = druidQuery.getAggregations()
                    .stream()
                    .map(aggregation -> AggregationType.getAggregation(aggregation, builder, ALIAS))
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

    private Map<String, Function<String, Object>> getAggregationTypeMapper(AbstractDruidAggregationQuery<?> druidQuery) {
        return druidQuery.getAggregations()
                .stream()
                .collect(Collectors.toMap(Aggregation::getName, aggregation -> {
                    Function<String, Object> typeMapper;
                    if (aggregation.getType().toLowerCase().contains("long")) {
                        typeMapper = Long::parseLong;
                    } else if (aggregation.getType().toLowerCase().contains("double")) {
                        typeMapper = Double::parseDouble;
                    } else {
                        typeMapper = Double::parseDouble;
                    }
                    return typeMapper;
                }));
    }

    private String relToSql(RelBuilder builder) {
        return relToSql.visitChild(0, builder.build()).asSelect().toString();
    }
}

