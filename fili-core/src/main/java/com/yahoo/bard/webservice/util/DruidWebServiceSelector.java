// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.handlers.RequestContext;

/**
 * Selects a DruidWebService to send a Druid query to, based on the query, request, and request context.
 */
public interface DruidWebServiceSelector {

    /**
     * Selects the Druid web service to send a Druid query to.
     *
     * @param context  The context for the Request
     * @param request  The request
     * @param druidQuery  The query to be sent to the service
     *
     * @return The web service to send the query to
     */
    DruidWebService select(RequestContext context, DataApiRequest request, DruidAggregationQuery<?> druidQuery);
}
