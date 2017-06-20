// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.sql.SqlBackedClient;
import com.yahoo.bard.webservice.sql.SqlConverter;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.LoggingContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Optional;

import javax.validation.constraints.NotNull;

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
        String user = "";
        String pass = "";
        try {
            sqlBackedClient = new SqlConverter(JdbcSchema.dataSource(dbUrl, driver, user, pass));
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
        Optional<Dimension> sqlDimension = request.getDimensions()
                .stream()
                .filter(dimension -> dimension.getApiName().equals("sql"))
                .findFirst();
        
        if (sqlBackedClient != null && sqlDimension.isPresent()) {
            SuccessCallback success = rootNode -> response.processResponse(
                    rootNode,
                    druidQuery,
                    new LoggingContext(RequestLog.copy())
            );

            FailureCallback failure = response.getFailureCallback(druidQuery);

            Dimension sqlFlag = sqlDimension.get();
            druidQuery.getDimensions().remove(sqlFlag);
            sqlBackedClient.executeQuery(druidQuery, success, failure);
        }
        return next.handleRequest(context, request, druidQuery, response);
    }
}
