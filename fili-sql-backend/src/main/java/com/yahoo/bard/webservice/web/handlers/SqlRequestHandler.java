// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.SqlBackedClient;
import com.yahoo.bard.webservice.SqlConverter;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.LoggingContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.istack.internal.NotNull;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A handler for queries made against a sql backend.
 */
public class SqlRequestHandler implements DataRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SqlRequestHandler.class);
    private final @NotNull DataRequestHandler next;
    private SqlBackedClient sqlBackedClient;

    /**
     * Constructor.
     *
     * @param next Next Handler in the chain
     */
    public SqlRequestHandler(DataRequestHandler next) {
        this.next = next;
        initializeSqlBackend();
    }

    /**
     * Initializes the connection to the sql backend and prepares
     * the {@link SqlBackedClient} for converting queries.
     */
    private void initializeSqlBackend() {
        // todo add settings for configuring a sql backed client
        String dbUrl = "jdbc:h2:mem:test";
        String driver = "org.h2.Driver";
        String user = null;
        String pass = null;
        try {
            Connection connection = DriverManager.getConnection(dbUrl, user, pass);
            sqlBackedClient = new SqlConverter(connection, JdbcSchema.dataSource(dbUrl, driver, user, pass));
        } catch (SQLException e) {
            LOG.error("Failed to set up sql backed client", e);
        }
    }

    @Override
    public boolean handleRequest(
            RequestContext context,
            DataApiRequest request,
            DruidAggregationQuery<?> druidQuery,
            ResponseProcessor response
    ) {
        //todo check if sql query
        if (sqlBackedClient != null) {
            try {
                Future<JsonNode> futureResponse = sqlBackedClient.executeQuery(druidQuery);
                response.processResponse(futureResponse.get(), druidQuery, new LoggingContext(RequestLog.copy()));
                return true;
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Failed to get response from sql backed client.", e);
                response.getErrorCallback(druidQuery)
                        .dispatch(HttpStatus.SC_INTERNAL_SERVER_ERROR, request.getFormat().name(), "FAILED");
            } catch (UnsupportedOperationException e) {
                response.getErrorCallback(druidQuery)
                        .dispatch(HttpStatus.SC_NOT_IMPLEMENTED, request.getFormat().name(), "UNSUPPORTED");
            }
        }
        return next.handleRequest(context, request, druidQuery, response);
    }
}
