// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.sun.istack.internal.NotNull;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

/**
 * A handler for queries made against a sql backend.
 */
public class SqlRequestHandler implements DataRequestHandler {
    private final @NotNull DataRequestHandler next;

    /**
     * Constructor.
     *
     * @param next  Next Handler in the chain
     */
    public SqlRequestHandler(DataRequestHandler next) {
        this.next = next;
    }

    @Override
    public boolean handleRequest(RequestContext context, DataApiRequest request, DruidAggregationQuery<?> druidQuery, ResponseProcessor response) {
        //todo check if sql query
        return next.handleRequest(context, request, druidQuery, response);
    }
}
