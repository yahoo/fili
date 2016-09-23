// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.validation.constraints.NotNull;

/**
 * Handler to return debugging of the request query without sending to druid.
 */
public class DebugRequestHandler extends BaseDataRequestHandler {

    protected final @NotNull DataRequestHandler next;

    /**
     * Build a debugging handler.
     *
     * @param next  The request handler to delegate the request to.
     * @param mapper  A JSON object mapper, used to parse the JSON response from the weight check.
     */
    public DebugRequestHandler(
            DataRequestHandler next,
            ObjectMapper mapper
    ) {
        super(mapper);
        this.next = next;
    }

    @Override
    public boolean handleRequest(
            final RequestContext context,
            final DataApiRequest request,
            final DruidAggregationQuery<?> druidQuery,
            final ResponseProcessor response
    ) {
        if (request.getFormat() != ResponseFormatType.DEBUG) {
            return next.handleRequest(context, request, druidQuery, response);
        }
        response.getErrorCallback(druidQuery).dispatch(200, request.getFormat().name(), "DEBUG");
        return true;
    }
}
