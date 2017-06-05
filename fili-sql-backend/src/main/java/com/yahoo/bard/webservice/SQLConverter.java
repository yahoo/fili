// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;


import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.QueryType;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery;
import com.yahoo.bard.webservice.mock.DruidResponse;
import com.yahoo.bard.webservice.mock.Mock;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts druid queries to sql, executes it, and returns a druid like response.
 */
public class SQLConverter {
    private static final Logger LOG = LoggerFactory.getLogger(SQLConverter.class);
    private static final RelToSqlConverter REL_TO_SQL = new RelToSqlConverter(SqlDialect.DUMMY);

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

        return query(druidQuery, generatedSql, connection);
    }

    public static JsonNode query(DruidQuery<?> druidQuery, String sql, Connection connection) throws Exception {
        LOG.debug("Executing \n{}", sql);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            return read(druidQuery, connection, resultSet);
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
     * @return druid-like result from query.
     */
    private static JsonNode read(DruidQuery<?> druidQuery, Connection connection, ResultSet resultSet)
            throws Exception {
        // result set cannot be reset after rows have been read, this consumes results by reading them
        Database.ResultSetFormatter rf = new Database.ResultSetFormatter();
        rf.resultSet(resultSet, 0);
        LOG.debug("Reading results \n{}", rf.string());

        int rows = 0;
        while (resultSet.next()) {
            ++rows;
            // process
        }
        LOG.debug("Fetched {} rows.", rows);

        ObjectMapper objectMapper = new ObjectMapper();
        LOG.debug("Original Query\n {}", objectMapper.valueToTree(druidQuery));
        DruidResponse druidResponse = Mock.druidResponse();
        JsonNode druidResponseJson = objectMapper.valueToTree(druidResponse);
        LOG.debug("Fake Druid Response\n {}", druidResponseJson);

        return druidResponseJson;
    }

    public static String buildTimeSeriesQuery(Connection connection, TimeSeriesQuery druidQuery, RelBuilder builder)
            throws SQLException {
        String name = druidQuery.getDataSource().getPhysicalTable().getTableName().asName();
        String timeCol = Database.getDateTimeColumn(connection, name);

        // druidQuery.getAggregations()
        //        .stream()
        //        .map(Aggregation::getDependentDimensions) // include dependent dimensions in select?

        builder.scan(name);

        if (druidQuery.getDimensions().size() != 0) {
            LOG.debug("Adding dimensions.");
            builder.project((RexInputRef[]) druidQuery.getDimensions()
                    .stream()
                    .map(Object::toString)
                    .map(builder::field)
                    .toArray());
        }

        List<RexNode> timefilters = druidQuery.getIntervals().stream().map(interval -> {
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
                        timefilters
                )
        );


        if (druidQuery.getAggregations().size() != 0) { // group by aggregations
            LOG.debug("Adding aggregations"); //are there any custom aggregations or can we just list them all in an
            // enum
            druidQuery.getAggregations().forEach(aggregation -> {
                builder.aggregate(
                        builder.groupKey(timeCol.toUpperCase()),
                        builder.sum(
                                false,
                                "ALIAS_" + aggregation.getFieldName(),
                                builder.field(aggregation.getFieldName())
                        )
                );
            });
        }

        // ImmutableList<RexNode> fields = builder.fields(); // the parts which are in SELECT
        builder.sort(builder.field(timeCol)); // order by time

        // where filters

        // how does this work with granularity? will this have to be bucketed by granularity here
        // sql aggregations are done with groupBy (do we have to worry about makers?)

        return relToSql(builder);
    }

    private static String relToSql(RelBuilder builder) {
        return REL_TO_SQL.visitChild(0, builder.build()).asSelect().toString();
    }

/* Example query to druid --------------------------------------------------------------------------
{
  "queryType": "timeseries",
  "dataSource": "sample_datasource",
  "granularity": "day",
  "descending": "true",
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "sample_dimension1", "value": "sample_value1" },
      { "type": "or",
        "fields": [
          { "type": "selector", "dimension": "sample_dimension2", "value": "sample_value2" },
          { "type": "selector", "dimension": "sample_dimension3", "value": "sample_value3" }
        ]
      }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "sample_name1", "fieldName": "sample_fieldName1" },
    { "type": "doubleSum", "name": "sample_name2", "fieldName": "sample_fieldName2" }
  ],
  "postAggregations": [
    { "type": "arithmetic",
      "name": "sample_divide",
      "fn": "/",
      "fields": [
        { "type": "fieldAccess", "name": "postAgg__sample_name1", "fieldName": "sample_name1" },
        { "type": "fieldAccess", "name": "postAgg__sample_name2", "fieldName": "sample_name2" }
      ]
    }
  ],
  "intervals": [ "2012-01-01T00:00:00.000/2012-01-03T00:00:00.000" ]
}
 --------------------------------------------------------------------------------------------------- */

    public static void main(String[] args) throws Exception {
        DruidAggregationQuery<?> druidQuery = Mock.timeSeriesQuery("WIKITICKER");
        JsonNode jsonNode = convert(druidQuery);

        // getSqlParser("select * from PERSON").parseQuery().toSqlString(SqlDialect.DUMMY)
        // todo validate?
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

