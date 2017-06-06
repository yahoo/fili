// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;


import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.QueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
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
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
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
    private static RelToSqlConverter relToSql;

    /**
     * No instances.
     */
    private SQLConverter() {

    }

    public static JsonNode convert(DruidAggregationQuery<?> druidQuery) throws Exception {
        LOG.debug("Processing druid query");
        QueryType queryType = druidQuery.getQueryType();
        if (DefaultQueryType.TIMESERIES.equals(queryType)) {
            TimeSeriesQuery timeSeriesQuery = (TimeSeriesQuery) druidQuery;
            return convert(timeSeriesQuery);
        }

        LOG.warn("Attempted to query unsupported type {}", queryType.toString());
        throw new RuntimeException("Unsupported query type");
    }

    public static JsonNode convert(TimeSeriesQuery druidQuery) throws Exception {
        LOG.debug("Processing time series query");

        Connection connection = Database.getDatabase();
        String generatedSql = buildTimeSeriesQuery(connection, druidQuery, builder());
        int timeGranularity = getTimeGranularity(druidQuery.getGranularity());

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
        DruidResponse<TimeseriesResult> timeseriesResultDruidResponse = new DruidResponse<>();
        while (resultSet.next()) {
            ++rows;
            ResultSetMetaData rsmd = resultSet.getMetaData();
            //read 3 columns and evaluate as time
            //create a druidResponse
            DateTime resultTimeStamp = new DateTime(DateTimeZone.UTC);
            if (timeGranularity >= 1) {
                int year = resultSet.getInt(1);
                resultTimeStamp = resultTimeStamp.withYear(year);
            }
            if (timeGranularity >= 2) {
                int month = resultSet.getInt(2);
                resultTimeStamp = resultTimeStamp.withMonthOfYear(month);
            } else {
                resultTimeStamp = resultTimeStamp.withMonthOfYear(0);
            }
            if (timeGranularity >= 3) {
                int weekOfYear = resultSet.getInt(3);
                resultTimeStamp = resultTimeStamp.withWeekOfWeekyear(weekOfYear);
            } else {
                resultTimeStamp = resultTimeStamp.withWeekOfWeekyear(0);
            }
            if (timeGranularity >= 4) {
                int dayOfYear = resultSet.getInt(4);
                resultTimeStamp = resultTimeStamp.withDayOfYear(dayOfYear);
            } else {
                resultTimeStamp = resultTimeStamp.withDayOfYear(0);
            }
            if (timeGranularity >= 5) {
                int hourOfDay = resultSet.getInt(5);
                resultTimeStamp = resultTimeStamp.withHourOfDay(hourOfDay);
            } else {
                resultTimeStamp = resultTimeStamp.withHourOfDay(0);
            }
            if (timeGranularity >= 6) {
                int minuteOfHour = resultSet.getInt(6);
                resultTimeStamp = resultTimeStamp.withMinuteOfHour(minuteOfHour);
            } else {
                resultTimeStamp = resultTimeStamp.withMinuteOfHour(0);
            }
            resultTimeStamp = resultTimeStamp.withSecondOfMinute(0).withMillisOfSecond(0);

            TimeseriesResult result = new TimeseriesResult(resultTimeStamp);
            Map<String, String> sqlResults = new HashMap<>();
            for (int i = timeGranularity + 1; i <= rsmd.getColumnCount(); i++) {
                String columnName = rsmd.getColumnName(i).replace(ALIAS, "");
                String val = resultSet.getString(i);
                sqlResults.put(columnName, val);
                result.add(columnName, resultMapper.get(columnName).apply(val));
            }

            druidQuery.getPostAggregations().forEach(postAggregation -> {
                Double postAggResult = PostAggregationEvaluator.evaluate(postAggregation, sqlResults);
                result.add(postAggregation.getName(), postAggResult);
            });

            timeseriesResultDruidResponse.results.add(result);
        }
        LOG.debug("Fetched {} rows.", rows);

        ObjectMapper objectMapper = new ObjectMapper();

        LOG.debug("Original Query\n {}", objectMapper.valueToTree(druidQuery));
        JsonNode druidResponseJson = objectMapper.valueToTree(timeseriesResultDruidResponse);
        LOG.debug("Fake Druid Response\n {}", druidResponseJson);

        return druidResponseJson;
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
        if (druidQuery.getDimensions().size() != 0) {
            LOG.debug("Adding dimensions { {} }", druidQuery.getDimensions());
            builder.project(druidQuery.getDimensions()
                    .stream()
                    .map(Object::toString)
                    .map(builder::field)
                    .toArray(RexInputRef[]::new));
        }
        // druidQuery.getAggregations()
        //        .stream()
        //        .map(Aggregation::getDependentDimensions) // include dependent dimensions in select?

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

        //this makes a group by on all the parts in the sublist
        List<RexNode> times = Arrays.asList(
                builder.call(SqlStdOperatorTable.YEAR, builder.field(timeCol)),
                builder.call(SqlStdOperatorTable.MONTH, builder.field(timeCol)),
                builder.call(SqlStdOperatorTable.WEEK, builder.field(timeCol)),
                builder.call(SqlStdOperatorTable.DAYOFYEAR, builder.field(timeCol)),
                builder.call(SqlStdOperatorTable.HOUR, builder.field(timeCol)),
                builder.call(SqlStdOperatorTable.MINUTE, builder.field(timeCol))
        );
        int timeGranularity = getTimeGranularity(druidQuery.getGranularity());

        //are there any custom aggregations or can we just list them all in an enum
        if (druidQuery.getAggregations().size() != 0) { // group by aggregations
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
                            times.subList(0, timeGranularity)
                            // todo group by queries have dimensions here
                    ),
                    // How to bucket time with granularity? UDF/SQL/manual in java? as of now sql
                    aggCalls
            );

        }

        // =============================================================================================

        // todo make something like PostAggregationEvaluator for filters
        // add WHERE/filters here

        // =============================================================================================

        // todo check descending
        builder.sort(builder.fields().subList(0, timeGranularity)); // order by same time as grouping

        // =============================================================================================

        // find non overlapping intervals to include in meta part of druids response?
        // this will have to be implemented later if at all since we don't have information about partial data

        return relToSql(builder);
    }

    private static int getTimeGranularity(Granularity granularity) {
        if (!(granularity instanceof DefaultTimeGrain)) {
            throw new IllegalStateException("Must be a DefaultTimeGrain");
        }
        DefaultTimeGrain timeGrain = (DefaultTimeGrain) granularity;
        switch (timeGrain) {
            case MINUTE:
                return 6;
            case HOUR:
                return 5;
            case DAY:
                return 4;
            case WEEK:
                return 3;
            case MONTH:
                return 2;
            case YEAR:
                return 1;
            case QUARTER:
                throw new IllegalStateException("Quarter timegrain not supported");
            default:
                throw new IllegalStateException("Timegrain not known " + timeGrain);
        }
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

