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
import com.yahoo.bard.webservice.sql.helper.CalciteHelper;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.ResponseFormatType;
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
    public static final String DATABASE_SCHEMA = SYSTEM_CONFIG.getPackageVariableName("database_schema");
    public static final String DATABASE_DRIVER = SYSTEM_CONFIG.getPackageVariableName("database_driver");
    public static final String DATABASE_URL = SYSTEM_CONFIG.getPackageVariableName("database_url");
    private final @NotNull DataRequestHandler next;
    private SqlBackedClient sqlConverter;

    /**
     * Constructor.
     *
     * @param next Next Handler in the chain
     * @param mapper
     */
    public SqlRequestHandler(
            DataRequestHandler next,
            ObjectMapper mapper,
            PhysicalTableDictionary physicalTableDictionary
    ) {
        this.next = next;
        initializeSqlBackend(mapper, physicalTableDictionary);
    }

    /**
     * Initializes the connection to the sql backend and prepares
     * the {@link SqlBackedClient} for converting queries.
     * @param mapper
     * @param physicalTableDictionary
     */
    private void initializeSqlBackend(
            ObjectMapper mapper,
            PhysicalTableDictionary physicalTableDictionary
    ) {
        String dbUrl = SYSTEM_CONFIG.getStringProperty(DATABASE_URL);
        String driver = SYSTEM_CONFIG.getStringProperty(DATABASE_DRIVER);
        String schema = SYSTEM_CONFIG.getStringProperty(DATABASE_SCHEMA);
        if (schema.isEmpty()) {
            schema = CalciteHelper.DEFAULT_SCHEMA;
        }
        String user = SYSTEM_CONFIG.getStringProperty(DATABASE_USERNAME);
        String pass = SYSTEM_CONFIG.getStringProperty(DATABASE_PASSWORD);
        try {
            sqlConverter = new SqlConverter(physicalTableDictionary, mapper, dbUrl, driver, user, pass, schema);
        } catch (SQLException e) {
            LOG.warn("Failed to initialize Sql backend", e);
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
        if (sqlConverter != null && request.getFormat().equals(ResponseFormatType.SQL)) {
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
