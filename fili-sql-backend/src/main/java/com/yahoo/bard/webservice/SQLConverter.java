// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;


import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery;
import com.yahoo.bard.webservice.mock.DruidResponse;
import com.yahoo.bard.webservice.mock.TimeseriesResult;

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
import org.h2.jdbc.JdbcSQLException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

/**
 * Converts druid queries to sql, executes it, and returns a druid like response.
 */
public class SQLConverter implements SqlBackedClient {
    private static final Logger LOG = LoggerFactory.getLogger(SQLConverter.class);
    public static final String THE_SCHEMA = "DEFAULT_SCHEMA";
    private static final String ALIAS = "ALIAS_";
    private static final ObjectMapper JSON_WRITER = new ObjectMapper();
    private final RelBuilder builder;
    private final RelToSqlConverter relToSql;
    private final Connection connection;

    public SQLConverter(Connection connection, DataSource dataSource) throws SQLException {
        this.connection = connection;
        relToSql = new RelToSqlConverter(SqlDialect.create(connection.getMetaData()));
        builder = builder(connection, dataSource);
    }

    @Override
    public Future<JsonNode> executeQuery(DruidQuery<?> druidQuery) {
        LOG.debug("Processing druid query");
        LOG.debug("Original Query\n {}", JSON_WRITER.valueToTree(druidQuery));

        DefaultQueryType queryType = (DefaultQueryType) druidQuery.getQueryType();
        JsonNode druidResponse = null;
        switch (queryType) {
            case TIMESERIES:
                TimeSeriesQuery timeSeriesQuery = (TimeSeriesQuery) druidQuery;
                try {
                    druidResponse = convert(timeSeriesQuery);
                } catch (IOException | SQLException e) {
                    LOG.error("Failed to process {}", druidQuery, e);
                    // todo throw on error
                }
        }

        if (druidResponse != null) {
            LOG.debug("Fake Druid Response\n {}", druidResponse);
            return new CompletedFuture<>(druidResponse, null);
        } else {
            LOG.warn("Attempted to query unsupported type {}", queryType.toString());
            throw new UnsupportedOperationException("Unsupported query type");
        }
    }

    private JsonNode convert(TimeSeriesQuery druidQuery) throws IOException, SQLException {
        LOG.debug("Processing time series query");

        String generatedSql = buildTimeSeriesQuery(connection, druidQuery);
        int timeGranularity = TimeConverter.getTimeGranularity(druidQuery.getGranularity());

        return query(druidQuery, generatedSql, timeGranularity);
    }

    private JsonNode query(TimeSeriesQuery druidQuery, String sql, int timeGranularity)
            throws SQLException {
        LOG.debug("Executing \n{}", sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            return read(druidQuery, resultSet, timeGranularity);
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
     * @param resultSet  the result set of the druid query.
     * @param timeGranularity
     * @return druid-like result from query.
     */
    private JsonNode read(
            TimeSeriesQuery druidQuery,
            ResultSet resultSet,
            int timeGranularity
    )
            throws SQLException {

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

    private String buildTimeSeriesQuery(Connection connection, TimeSeriesQuery druidQuery)
            throws SQLException {
        String name = druidQuery.getDataSource().getPhysicalTable().getName();
        String timeCol = DatabaseHelper.getDateTimeColumn(connection, name).toUpperCase();

        // =============================================================================================

        builder.scan(name); // choose table

        // =============================================================================================

        // building filter and getting dimensions
        List<String> dimensions = FilterEvaluator.getDimensionNames(builder, druidQuery.getFilter());

        // =============================================================================================

        List<RexInputRef> selectedColumns = druidQuery.getAggregations()
                .stream()
                .map(Aggregation::getFieldName)
                .map(builder::field)
                .collect(Collectors.toList());

        selectedColumns.add(builder.field(timeCol));

        List<RexInputRef> dimensionsInFilters = dimensions
                .stream()
                .map(builder::field)
                .collect(Collectors.toList());
        selectedColumns.addAll(dimensionsInFilters);

        LOG.debug("Selecting needed dimensions");
        builder.project(selectedColumns);

        // =============================================================================================

        LOG.debug("Adding filter");
        // todo add this filter below with the time filters, this may unravel an unneeded nesting
        FilterEvaluator.addFilter(builder, druidQuery.getFilter());

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

        Collection<RexNode> timeFilterRexNodes = Collections.emptyList();
        if (timeFilters.size() > 1) {
            timeFilterRexNodes = Collections.singleton(builder.call(
                    SqlStdOperatorTable.OR,
                    timeFilters
            ));
        } else if (timeFilters.size() == 1) {
            timeFilterRexNodes = timeFilters;
        }

        builder.filter(timeFilterRexNodes);

        // =============================================================================================

        //are there any custom aggregations or can we just list them all in an enum?
        if (druidQuery.getAggregations().size() != 0) {
            LOG.debug("Adding aggregations { {} }", druidQuery.getAggregations());

            List<RelBuilder.AggCall> druidAggregations = druidQuery.getAggregations()
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
                            TimeConverter.getGroupByForGranularity(builder, druidQuery.getGranularity(), timeCol)
                            // todo groupBy queries will need dimensions here
                    ),
                    druidAggregations
            );
        }

        // =============================================================================================

        // todo check descending
        //this makes a group by on all the parts from the time sublist
        // somewhat bad, todo look into getting rexnode references and using them down here
        int timeGranularity = TimeConverter.getTimeGranularity(druidQuery.getGranularity());
        builder.sort(builder.fields().subList(0, timeGranularity)); // order by same time as grouping

        // =============================================================================================

        // NOTE: does not include missing interval meta information
        // this will have to be implemented later if at all since we don't know about partial data

        return relToSql(builder);
    }

    private String relToSql(RelBuilder builder) {
        return relToSql.visitChild(0, builder.build()).asSelect().toString();
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
                JdbcSchema.create(rootSchema, null, dataSource, null, THE_SCHEMA)
        );
    }
}

