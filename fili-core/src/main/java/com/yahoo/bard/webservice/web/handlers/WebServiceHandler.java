// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

/**
 * WebServiceHandler carries the web service through the chain of request handlers so we know who to send the request
 * to when the request handlers finish.
 */
public class WebServiceHandler implements DataRequestHandler {
    private final DruidWebService webService;
    private final DataRequestHandler next;

    /**
     * Constructor.
     *
     * @param webService  WebService to provide when asked
     * @param next  Next Handler in the chain
     */
    public WebServiceHandler(DruidWebService webService, DataRequestHandler next) {
        this.webService = webService;
        this.next = next;
    }

    public DruidWebService getWebService() {
        return webService;
    }

    @Override
    public boolean handleRequest(
            RequestContext context,
            DataApiRequest request,
            DruidAggregationQuery<?> druidQuery,
            ResponseProcessor response
    ) {
        return next.handleRequest(context, request, druidQuery, response);
    }
}
