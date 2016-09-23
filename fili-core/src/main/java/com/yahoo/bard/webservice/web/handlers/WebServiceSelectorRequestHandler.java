// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.QueryContext;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The Webservice selector routes requests in one of several ways, described below.
 * <ul>
 *     <li>the default behavior which selects between a ui or a non-ui web service based on request headers</li>
 *     <li>a custom implementation of {@link com.yahoo.bard.webservice.web.handlers.WebServiceHandlerSelector}</li>
 * </ul>
 */
public class WebServiceSelectorRequestHandler extends BaseDataRequestHandler {

    private final WebServiceHandlerSelector handlerSelector;

    /**
     * Constructor.
     *
     * @param uiWebService  UI Web Service
     * @param nonUiWebService  Non-UI Web Service
     * @param uiWebserviceNext  Handler for the UI path
     * @param nonUiWebserviceNext  Handler for the non-UI path
     * @param mapper  Mapper to use when processing JSON
     */
    public WebServiceSelectorRequestHandler(
            DruidWebService uiWebService,
            DruidWebService nonUiWebService,
            DataRequestHandler uiWebserviceNext,
            DataRequestHandler nonUiWebserviceNext,
            ObjectMapper mapper
    ) {
        this(
                new DefaultWebServiceHandlerSelector(
                        uiWebService,
                        nonUiWebService,
                        uiWebserviceNext,
                        nonUiWebserviceNext
                ),
                mapper
        );
    }

    /**
     * Constructor.
     *
     * @param handlerSelector  Handler selector to use when selecting a web service
     * @param mapper  Mapper to use when processing JSON
     */
    public WebServiceSelectorRequestHandler(WebServiceHandlerSelector handlerSelector, ObjectMapper mapper) {
        super(mapper);
        this.handlerSelector = handlerSelector;
    }

    @Override
    public boolean handleRequest(
            RequestContext context,
            DataApiRequest request,
            DruidAggregationQuery<?> druidQuery,
            ResponseProcessor response
    ) {
        WebServiceHandler handler = handlerSelector.select(druidQuery, request, context);
        // Add a timeout to the query if the selected webService is configured with one
        Integer timeout = handler.getWebService().getTimeout();
        // Add a priority to the query if there is one configured
        Integer priority = handler.getWebService().getServiceConfig().getPriority();
        QueryContext qc = druidQuery.getContext().withTimeout(timeout).withPriority(priority);
        if (!qc.isEmpty()) {
            druidQuery = druidQuery.withContext(qc);
        }
        return handler.handleRequest(context, request, druidQuery, response);
    }
}
