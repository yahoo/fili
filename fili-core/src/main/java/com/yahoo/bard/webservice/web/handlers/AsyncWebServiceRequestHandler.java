// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfo;
import com.yahoo.bard.webservice.util.DruidWebServiceSelector;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.LoggingContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.validation.constraints.NotNull;

/**
 * Request handler to submit the response to the druid web service.
 */
public class AsyncWebServiceRequestHandler extends BaseDataRequestHandler {

    protected final @NotNull DruidWebServiceSelector druidWebServiceSelector;

    /**
     * Builds the request handler.
     *
     * @param druidWebServiceSelector  A function to dynamically select the Druid webservice to query
     * @param mapper  The map for all JSON processing
     */
    public AsyncWebServiceRequestHandler(DruidWebServiceSelector druidWebServiceSelector, ObjectMapper mapper) {
        super(mapper);
        this.druidWebServiceSelector = druidWebServiceSelector;
    }

    /**
     * Build the request handler.
     *
     * @param druidWebService  The target web service for the request
     * @param mapper  The mapper for all JSON processing
     */
    public AsyncWebServiceRequestHandler(DruidWebService druidWebService, ObjectMapper mapper) {
        this((i1, i2, i3) -> druidWebService, mapper);
    }

    @Override
    public boolean handleRequest(
            final RequestContext context,
            final DataApiRequest request,
            final DruidAggregationQuery<?> druidQuery,
            final ResponseProcessor response
    ) {
        SuccessCallback success = new SuccessCallback() {
            @Override
            public void invoke(JsonNode rootNode) {
                response.processResponse(rootNode, druidQuery, new LoggingContext(RequestLog.copy()));
            }
        };
        HttpErrorCallback error = response.getErrorCallback(druidQuery);
        FailureCallback failure = response.getFailureCallback(druidQuery);

        BardQueryInfo.incrementCountFactHits();
        DruidWebService webService = druidWebServiceSelector.select(context, request, druidQuery);
        webService.postDruidQuery(context, success, error, failure, druidQuery);
        return true;
    }
}
