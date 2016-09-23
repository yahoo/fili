// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.DataApiRequestTypeIdentifier;

import com.codahale.metrics.Meter;

/**
 * Selects between ui and non-ui web services based on request headers.
 */
public class DefaultWebServiceHandlerSelector implements WebServiceHandlerSelector {

    public static final Meter QUERY_REQUEST_TOTAL = MetricRegistryFactory.getRegistry().meter("queries.meter.total");

    private final WebServiceHandler uiWebServiceHandler;
    private final WebServiceHandler nonUiWebServiceHandler;

    /**
     * Constructor.
     *
     * @param uiWebService  UI Web Service
     * @param nonUiWebService  Non-UI Web Service
     * @param uiWebServiceNext  Handler for the UI path
     * @param nonUiWebServiceNext  Handler for the non-UI path
     */
    public DefaultWebServiceHandlerSelector(
            DruidWebService uiWebService,
            DruidWebService nonUiWebService,
            DataRequestHandler uiWebServiceNext,
            DataRequestHandler nonUiWebServiceNext
    ) {
        uiWebServiceHandler = new WebServiceHandler(uiWebService, uiWebServiceNext);
        nonUiWebServiceHandler = new WebServiceHandler(nonUiWebService, nonUiWebServiceNext);
    }

    @Override
    public WebServiceHandler select(
            DruidAggregationQuery<?> druidQuery,
            DataApiRequest request,
            RequestContext context
    ) {
        QUERY_REQUEST_TOTAL.mark();
        return DataApiRequestTypeIdentifier.isUi(context.getHeadersLowerCase()) ?
                uiWebServiceHandler :
                nonUiWebServiceHandler;
    }
}
