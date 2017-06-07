// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;


import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery;
import com.yahoo.bard.webservice.mock.DruidResponse;
import com.yahoo.bard.webservice.mock.Simple;
import com.yahoo.bard.webservice.mock.TimeseriesResult;
import com.yahoo.bard.webservice.test.Database;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
import org.h2.jdbc.JdbcSQLException;
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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Converts druid queries to sql, executes it, and returns a druid like response.
 */
public class SQLConverter {
    private static final Logger LOG = LoggerFactory.getLogger(SQLConverter.class);
    private static final String ALIAS = "ALIAS_";
    private static final ObjectMapper JSON_WRITER = new ObjectMapper();
    private static RelToSqlConverter relToSql;

    /**
     * No instances.
     */
    private SQLConverter() {

    }

    public static JsonNode convert(DruidAggregationQuery<?> druidQuery) throws Exception {
        LOG.debug("Processing druid query");
        LOG.debug("Original Query\n {}", JSON_WRITER.valueToTree(druidQuery));

        DefaultQueryType queryType = (DefaultQueryType) druidQuery.getQueryType();
        JsonNode druidResponse = null;
        switch (queryType) {
            case TIMESERIES:
                TimeSeriesQuery timeSeriesQuery = (TimeSeriesQuery) druidQuery;
                druidResponse = convert(timeSeriesQuery);
        }

        if (druidResponse != null) {
            LOG.debug("Fake Druid Response\n {}", druidResponse);
            return druidResponse;
        } else {
            LOG.warn("Attempted to query unsupported type {}", queryType.toString());
            throw new UnsupportedOperationException("Unsupported query type");
        }
    }

    public static JsonNode convert(TimeSeriesQuery druidQuery) throws Exception {
        LOG.debug("Processing time series query");

        Connection connection = Database.getDatabase();
        String generatedSql = buildTimeSeriesQuery(connection, druidQuery, builder());
        int timeGranularity = TimeConverter.getTimeGranularity(druidQuery.getGranularity());

        return query(druidQuery, generatedSql, connection, timeGranularity);
    }

    public static JsonNode query(TimeSeriesQuery druidQuery, String sql, Connection connection, int timeGranularity)
            throws Exception {
        LOG.debug("Executing \n{}", sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            return read(druidQuery, connection, resultSet, timeGranularity);
        } catch (JdbcSQLException e) {
            LOG.warn("Failed to query database {} with {}", connection.getCatalog(), sql);
            throw new RuntimeException("Could not finish query", e);
        }
    }

    /**
     * Reads the result set and converts it into a result that druid
     * would produce.
     *
     * @param druidQuery the druid query to be made.
     * @param connection the connection to the database.
     * @param resultSet  the result set of the druid query.
     * @param timeGranularity
     * @return druid-like result from query.
     */
    private static JsonNode read(
            TimeSeriesQuery druidQuery,
            Connection connection,
            ResultSet resultSet,
            int timeGranularity
    )
            throws Exception {
        // result set cannot be reset after rows have been read, this consumes results by reading them
        // Database.ResultSetFormatter rf = new Database.ResultSetFormatter();
        // rf.resultSet(resultSet);
        // LOG.debug("Reading results \n{}", rf.string());
        Map<String, Function<String, Object>> resultMapper = druidQuery.getAggregations()
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


        int rows = 0;
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        DruidResponse<TimeseriesResult> timeseriesResultDruidResponse = new DruidResponse<>();
        while (resultSet.next()) {
            ++rows;

            DateTime resultTimeStamp = TimeConverter.getDateTime(resultSet, timeGranularity);
            TimeseriesResult rowResult = new TimeseriesResult(resultTimeStamp);
            Map<String, String> sqlResults = new HashMap<>();

            for (int i = timeGranularity + 1; i <= resultSetMetaData.getColumnCount(); i++) {
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

            timeseriesResultDruidResponse.results.add(rowResult);
        }
        LOG.debug("Fetched {} rows.", rows);

        return JSON_WRITER.valueToTree(timeseriesResultDruidResponse);
    }

    public static String buildTimeSeriesQuery(Connection connection, TimeSeriesQuery druidQuery, RelBuilder builder)
            throws SQLException {
        initRelToSqlConverter(connection);

        String name = druidQuery.getDataSource().getPhysicalTable().getName();
        String timeCol = Database.getDateTimeColumn(connection, name).toUpperCase();

        // =============================================================================================

        builder.scan(name); // choose table

        // =============================================================================================

        // select dimensions/metrics? This section might not be needed

        List<RexInputRef> collect = druidQuery.getAggregations()
                .stream()
                .map(Aggregation::getFieldName)
                .map(Object::toString)
                .map(builder::field)
                .collect(Collectors.toList());
        collect.add(builder.field(timeCol));

        collect.addAll(
                FilterEvaluator.getDimensionNames(druidQuery.getFilter())
                        .stream()
                        .map(builder::field)
                        .collect(Collectors.toList())
        );

        LOG.debug("Selecting needed dimensions");
        builder.project(collect);

        // =============================================================================================

        FilterEvaluator.add(builder, druidQuery.getFilter());

        // =============================================================================================

        // create filters to only select results within the given intervals
        List<RexNode> timeFilters = druidQuery.getIntervals().stream().map(interval -> {
            Timestamp start = TimeUtils.timestampFromMillis(interval.getStartMillis());
            Timestamp end = TimeUtils.timestampFromMillis(interval.getEndMillis());

            return builder.call(
                    SqlStdOperatorTable.AND,
                    builder.call(
                            SqlStdOperatorTable.GREATER_THAN,
                            builder.field(timeCol),
                            builder.literal(start.toString())
                    ),
                    builder.call(
                            SqlStdOperatorTable.LESS_THAN,
                            builder.field(timeCol),
                            builder.literal(end.toString())
                    )
            );
        }).collect(Collectors.toList());
        builder.filter(
                builder.call(
                        SqlStdOperatorTable.OR,
                        timeFilters
                )
        );

        // =============================================================================================

        //are there any custom aggregations or can we just list them all in an enum?
        if (druidQuery.getAggregations().size() != 0) {
            LOG.debug("Adding aggregations { {} }", druidQuery.getAggregations());

            List<RelBuilder.AggCall> aggCalls = druidQuery.getAggregations()
                    .stream()
                    .map(aggregation -> {
                        return AggregationType.getAggregation(
                                AggregationType.fromDruidType(aggregation.getType()),
                                builder,
                                ALIAS,
                                aggregation.getFieldName()
                        );
                    })
                    .collect(Collectors.toList());

            builder.aggregate(
                    builder.groupKey(
                            TimeConverter.getRexNodes(builder, druidQuery.getGranularity(), timeCol)
                            // todo groupBy queries will need dimensions here
                    ),
                    // How to bucket time with granularity? UDF/SQL/manual in java? as of now sql
                    aggCalls
            );

        }

        // =============================================================================================

        // todo check descending
        //this makes a group by on all the parts in the sublist
        int timeGranularity = TimeConverter.getTimeGranularity(druidQuery.getGranularity());
        builder.sort(builder.fields().subList(0, timeGranularity)); // order by same time as grouping

        // =============================================================================================

        // find non overlapping intervals to include in meta part of druids response?
        // this will have to be implemented later if at all since we don't have information about partial data

        return relToSql(builder);
    }

    private static void initRelToSqlConverter(final Connection connection) throws SQLException {
        if (relToSql == null) {
            relToSql = new RelToSqlConverter(SqlDialect.create(connection.getMetaData()));
        }
    }

    private static String relToSql(RelBuilder builder) {
        return relToSql.visitChild(0, builder.build()).asSelect().toString();
    }


    public static void main(String[] args) throws Exception {
        DruidAggregationQuery<?> druidQuery = Simple.timeSeriesQuery("WIKITICKER");
        JsonNode jsonNode = convert(druidQuery);
    }

    public static RelBuilder builder() {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        return RelBuilder.create(
                Frameworks.newConfigBuilder()
                        .parserConfig(SqlParser.Config.DEFAULT)
                        .defaultSchema(addSchema(rootSchema))
                        .traitDefs((List<RelTraitDef>) null)
                        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
                        .build()
        );
    }

    public static SchemaPlus addSchema(SchemaPlus rootSchema) {
        return rootSchema.add(
                Database.THE_SCHEMA,
                JdbcSchema.create(rootSchema, null, Database.getDataSource(), null, Database.THE_SCHEMA)
        );
    }

}

