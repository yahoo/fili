// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.sql.SqlAggregationQuery;
import com.yahoo.bard.webservice.sql.SqlBackedClient;
import com.yahoo.bard.webservice.table.SqlPhysicalTable;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.LoggingContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

/**
 * A handler for queries made against a sql backend.
 */
public class PrestoRequestHandler implements DataRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PrestoRequestHandler.class);
    private final @NotNull DataRequestHandler next;
    private SqlBackedClient prestoBackedClient;

    /**
     * Constructor.
     *
     * @param next Next Handler in the chain
     * @param prestoBackedClient the client instance for presto
     */
    public PrestoRequestHandler(DataRequestHandler next, SqlBackedClient prestoBackedClient) {
        this.next = next;
        this.prestoBackedClient = prestoBackedClient;
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
        if (isSqlBacked) {
            LoggingContext copy = new LoggingContext(RequestLog.copy());
            SuccessCallback success = rootNode -> {
                response.processResponse(
                        rootNode,
                        new SqlAggregationQuery(druidQuery),
                        copy
                );
            };
            FailureCallback failure = response.getFailureCallback(druidQuery);
            LOG.warn("processing {}", druidQuery);

            prestoBackedClient.executeQuery(druidQuery, success, failure);

            return true;
        }

        return next.handleRequest(context, request, druidQuery, response);
    }
}
