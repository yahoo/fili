// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.sql.DefaultSqlBackedClient;
import com.yahoo.bard.webservice.sql.SqlAggregationQuery;
import com.yahoo.bard.webservice.sql.SqlBackedClient;
import com.yahoo.bard.webservice.table.SqlPhysicalTable;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.LoggingContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

import javax.validation.constraints.NotNull;

/**
 * A handler for queries made against a sql backend.
 */
public class SqlRequestHandler implements DataRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SqlRequestHandler.class);
    public static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    public static final String DATABASE_PASSWORD = SYSTEM_CONFIG.getPackageVariableName("database_password");
    public static final String DATABASE_USERNAME = SYSTEM_CONFIG.getPackageVariableName("database_username");
    public static final String DATABASE_DRIVER = SYSTEM_CONFIG.getPackageVariableName("database_driver");
    public static final String DATABASE_URL = SYSTEM_CONFIG.getPackageVariableName("database_url");
    private final @NotNull DataRequestHandler next;
    private SqlBackedClient sqlConverter;

    /**
     * Constructor.
     *
     * @param next Next Handler in the chain
     * @param mapper  The mapper for all JSON processing
     */
    public SqlRequestHandler(DataRequestHandler next, ObjectMapper mapper) {
        this.next = next;
        initializeSqlBackend(mapper);
    }

    /**
     * Initializes the connection to the sql backend and prepares
     * the {@link SqlBackedClient} for converting queries.
     *
     * @param mapper  The mapper for all JSON processing.
     */
    private void initializeSqlBackend(ObjectMapper mapper) {
        String dbUrl = SYSTEM_CONFIG.getStringProperty(DATABASE_URL);
        String driver = SYSTEM_CONFIG.getStringProperty(DATABASE_DRIVER);
        String user = SYSTEM_CONFIG.getStringProperty(DATABASE_USERNAME);
        String pass = SYSTEM_CONFIG.getStringProperty(DATABASE_PASSWORD);
        try {
            sqlConverter = new DefaultSqlBackedClient(dbUrl, driver, user, pass, mapper);
        } catch (SQLException e) {
            LOG.warn("Failed to initialize Sql backend", e);
        }
    }

    /**
     * Handles a request by detecting if it's a sql backed table and sending to a sql backend.
     *
     * @param context  The context for the Request.
     * @param request  The Api Request Object.
     * @param druidQuery  The druid query.
     * @param response  The Async response.
     *
     * @return true if request was handled.
     */
    @Override
    public boolean handleRequest(
            RequestContext context,
            DataApiRequest request,
            DruidAggregationQuery<?> druidQuery,
            ResponseProcessor response
    ) {
        boolean isSqlBacked = druidQuery.getDataSource()
                .getPhysicalTable()
                .getSourceTable() instanceof SqlPhysicalTable;

        if (sqlConverter != null && isSqlBacked) {
            LOG.info("Intercepting for sql backend");
            LoggingContext copy = new LoggingContext(RequestLog.copy());
            SuccessCallback success = rootNode -> {
                response.processResponse(
                        rootNode,
                        new SqlAggregationQuery(druidQuery),
                        copy
                );
            };
            FailureCallback failure = response.getFailureCallback(druidQuery);

            sqlConverter.executeQuery(druidQuery, success, failure);

            return true;
        }

        return next.handleRequest(context, request, druidQuery, response);
    }
}
