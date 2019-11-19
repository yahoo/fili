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
     *
     * @param dataSource  The dataSource for the jdbc schema.
     * @param objectMapper  The mapper for all JSON processing.
     *
     * @throws SQLException if can't read from database.
     */
    public DefaultSqlBackedClient(DataSource dataSource, ObjectMapper objectMapper) throws SQLException {
        calciteHelper = new CalciteHelper(dataSource);
        druidQueryToSqlConverter = new DruidQueryToSqlConverter(calciteHelper);
        jsonWriter = objectMapper;
    }

    /**
     * Creates a sql converter using the given database and datasource.
     * TODO See https://github.com/yahoo/fili/issues/511
     *
     * @param url  The url location of the database to connect to.
     * @param driver  The driver (i.e. "org.h2.Driver") to connect with.
     * @param username  The username to connect to the database.
     * @param password  The password to connect to the database.
     * @param objectMapper  The mapper for all JSON processing.
     *
     * @throws SQLException if can't read from database.
     */
    public DefaultSqlBackedClient(
            String url,
            String driver,
            String username,
            String password,
            ObjectMapper objectMapper
    ) throws SQLException {
        DataSource dataSource = JdbcSchema.dataSource(url, driver, username, password);
        calciteHelper = new CalciteHelper(dataSource);
        druidQueryToSqlConverter = new DruidQueryToSqlConverter(calciteHelper);
        jsonWriter = objectMapper;
    }

    @Override
    public Future<JsonNode> executeQuery(
            DruidQuery<?> druidQuery,
            SuccessCallback successCallback,
            FailureCallback failureCallback
    ) {
        RequestLog logCtx = RequestLog.dump();
        //todo eventually stop/start RequestLog phases
        return CompletableFuture.supplyAsync(() -> {
                    try {
                        JsonNode jsonNode = executeAndProcessQuery((DruidAggregationQuery) druidQuery);
                        if (successCallback != null) {
                            successCallback.invoke(jsonNode);
                        }
                        return jsonNode;
                    } catch (Throwable t) {
                        LOG.warn("Failed while querying ", t);
                        if (failureCallback != null) {
                            failureCallback.dispatch(t);
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

        String sqlQuery = druidQueryToSqlConverter.buildSqlQuery(druidQuery, aliasMaker);
        LOG.debug("Executing \n{}", sqlQuery);

        SqlResultSetProcessor resultSetProcessor = new SqlResultSetProcessor(
                druidQuery,
                aliasMaker,
                jsonWriter,
                druidQueryToSqlConverter.getTimeConverter()
        );

        try (Connection connection = calciteHelper.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            resultSetProcessor.process(resultSet);
            JsonNode jsonNode = resultSetProcessor.buildDruidResponse();
            LOG.trace("Created response: {}", jsonNode);
            return jsonNode;
        } catch (SQLException e) {
            LOG.warn("Failed while processing {}", druidQuery);
            throw new RuntimeException("Couldn't generate sql", e);
        }
    }
}
