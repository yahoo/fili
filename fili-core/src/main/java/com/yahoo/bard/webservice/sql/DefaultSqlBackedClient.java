// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql;


import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.sql.helper.CalciteHelper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import javax.sql.DataSource;

/**
 * Converts druid queries to sql, executes it, and returns a druid like response.
 */
public class DefaultSqlBackedClient implements SqlBackedClient {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSqlBackedClient.class);
    private final ObjectMapper jsonWriter;
    private final DruidQueryToSqlConverter druidQueryToSqlConverter;
    private final CalciteHelper calciteHelper;

    /**
     * Creates a sql converter using the given database and datasource.
     * The default schema is "PUBLIC" (i.e. you haven't called "create schema"
     * and "set schema")
     *
     * @param dataSource  The dataSource for the jdbc schema.
     * @param objectMapper  The mapper for all JSON processing.
     *
     * @throws SQLException if can't read from database.
     */
    public DefaultSqlBackedClient(DataSource dataSource, ObjectMapper objectMapper) throws SQLException {
        calciteHelper = new CalciteHelper(dataSource, CalciteHelper.DEFAULT_SCHEMA);
        druidQueryToSqlConverter = new DruidQueryToSqlConverter(calciteHelper);
        jsonWriter = objectMapper;
    }

    /**
     * Creates a sql converter using the given database and datasource.
     *
     * @param url  The url location of the database to connect to.
     * @param driver  The driver (i.e. "org.h2.Driver") to connect with.
     * @param username  The username to connect to the database.
     * @param password  The password to connect to the database.
     * @param schemaName  The name of the schema used for the database.
     * @param objectMapper  The mapper for all JSON processing.
     *
     * @throws SQLException if can't read from database.
     */
    public DefaultSqlBackedClient(
            String url,
            String driver,
            String username,
            String password,
            String schemaName,
            ObjectMapper objectMapper
    ) throws SQLException {
        DataSource dataSource = JdbcSchema.dataSource(url, driver, username, password);
        //todo remove schema
        calciteHelper = new CalciteHelper(dataSource, schemaName);
        druidQueryToSqlConverter = new DruidQueryToSqlConverter(calciteHelper);
        jsonWriter = objectMapper;
    }

    @Override
    public Future<JsonNode> executeQuery(
            DruidQuery<?> druidQuery,
            SuccessCallback successCallback,
            FailureCallback failureCallback
    ) {
        final RequestLog logCtx = RequestLog.dump();
        return CompletableFuture.supplyAsync(() -> {
                    try {
                        JsonNode jsonNode = executeAndProcessQuery((DruidAggregationQuery) druidQuery);
                        if (successCallback != null) {
                            successCallback.invoke(jsonNode);
                        }
                        return jsonNode;
                    } catch (RuntimeException e) {
                        LOG.warn("Failed while querying ", e);
                        if (failureCallback != null) {
                            failureCallback.dispatch(e);
                        }
                    } finally {
                        RequestLog.restore(logCtx);
                    }
                    return null;
                }
        );
    }

    /**
     * Builds sql for a druid query, execute it against the database, process
     * the results and return a jsonNode in the format of a druid response.
     *
     * @param druidQuery  The druid query to build and process.
     *
     * @return a druid-like response to the query.
     */
    private JsonNode executeAndProcessQuery(DruidAggregationQuery<?> druidQuery) {
        if (!druidQueryToSqlConverter.isValidQuery(druidQuery)) {
            throw new UnsupportedOperationException("Unable to process " + druidQuery);
        }

        ApiToFieldMapper aliasMaker = new ApiToFieldMapper(druidQuery.getDataSource().getPhysicalTable().getSchema());

        try (Connection connection = calciteHelper.getConnection()) {
            String sqlQuery = druidQueryToSqlConverter.buildSqlQuery(connection, druidQuery, aliasMaker);
            LOG.debug("Executing \n{}", sqlQuery);

            SqlResultSetProcessor resultSetProcessor = new SqlResultSetProcessor(
                    druidQuery,
                    aliasMaker,
                    jsonWriter,
                    druidQueryToSqlConverter.getTimeConverter()
            );

            try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                resultSetProcessor.addResultSet(resultSet);

            } catch (SQLException e) {
                LOG.warn("Failed to read SQL ResultSet for query {}", druidQuery);
                throw new RuntimeException("Could not finish query", e);
            }

            JsonNode jsonNode = resultSetProcessor.process();
            LOG.trace("Created response: {}", jsonNode);
            return jsonNode;

        } catch (SQLException e) {
            throw new RuntimeException("Couldn't generate sql", e);
        }
    }
}
