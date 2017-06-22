// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.sql.SqlAggregationQuery;
import com.yahoo.bard.webservice.sql.SqlBackedClient;
import com.yahoo.bard.webservice.sql.SqlConverter;
import com.yahoo.bard.webservice.sql.database.Database;
import com.yahoo.bard.webservice.sql.helper.CalciteHelper;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.responseprocessors.LoggingContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

import javax.validation.constraints.NotNull;

/**
 * A handler for queries made against a sql backend.
 */
public class SqlRequestHandler implements DataRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SqlRequestHandler.class);
    private final @NotNull DataRequestHandler next;
    private SqlBackedClient sqlConverter;

    /**
     * Constructor.
     *
     * @param next Next Handler in the chain
     * @param mapper
     */
    public SqlRequestHandler(DataRequestHandler next, ObjectMapper mapper) {
        this.next = next;
        initializeSqlBackend(mapper);
    }

    /**
     * Initializes the connection to the sql backend and prepares
     * the {@link SqlBackedClient} for converting queries.
     * @param mapper
     */
    private void initializeSqlBackend(ObjectMapper mapper) {
        SystemConfig systemConfig = SystemConfigProvider.getInstance();
        String dbUrl = systemConfig.getStringProperty(systemConfig.getPackageVariableName("database_url"));
        String driver = systemConfig.getStringProperty(systemConfig.getPackageVariableName("database_driver"));
        String schema = systemConfig.getStringProperty(systemConfig.getPackageVariableName("database_schema"));
        if (schema.isEmpty()) {
            schema = CalciteHelper.DEFAULT_SCHEMA;
        }
        String user = systemConfig.getStringProperty(systemConfig.getPackageVariableName("database_username"));
        String pass = systemConfig.getStringProperty(systemConfig.getPackageVariableName("database_password"));
        try {
            Database.initializeDatabase();
            sqlConverter = new SqlConverter(mapper, dbUrl, driver, user, pass, schema);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean handleRequest(
            RequestContext context,
            DataApiRequest request,
            DruidAggregationQuery<?> druidQuery,
            ResponseProcessor response
    ) {
        //todo better check for sql query
        request.getFilter();
        if (request.getFormat().equals(ResponseFormatType.SQL)) {
            LOG.info("Intercepting for sql backend");
            SuccessCallback success = rootNode -> response.processResponse(
                    rootNode,
                    new SqlAggregationQuery(druidQuery),
                    new LoggingContext(RequestLog.copy())
            );
            FailureCallback failure = response.getFailureCallback(druidQuery);

            sqlConverter.executeQuery(druidQuery, success, failure);

            return true;
        }

        return next.handleRequest(context, request, druidQuery, response);
    }
}
