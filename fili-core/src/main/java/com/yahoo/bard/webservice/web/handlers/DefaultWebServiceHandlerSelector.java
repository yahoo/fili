// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.DataApiRequest;

import com.codahale.metrics.Meter;

/**
 * A no-op web service selector.
 */
public class DefaultWebServiceHandlerSelector implements WebServiceHandlerSelector {

    public static final Meter QUERY_REQUEST_TOTAL = MetricRegistryFactory.getRegistry().meter("queries.meter.total");

    private final WebServiceHandler webServiceHandler;

    /**
     * Constructor.
     *
     * @param webService  UI Web Service
     * @param webServiceNext  Handler for the UI path
     */
    public DefaultWebServiceHandlerSelector(
            DruidWebService webService,
            DataRequestHandler webServiceNext
    ) {
        webServiceHandler = new WebServiceHandler(webService, webServiceNext);
    }

    @Override
    public WebServiceHandler select(
            DruidAggregationQuery<?> druidQuery,
            DataApiRequest request,
            RequestContext context
    ) {
        QUERY_REQUEST_TOTAL.mark();
        return webServiceHandler;
    }
}
