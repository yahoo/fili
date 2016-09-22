// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.DataApiRequest;

/**
 * WebServiceHandlerSelectors are responsible for choosing the right web service with the right broker URL to call.
 * One of the example use-cases is to have an implementation give an API instance the ability to hit multiple
 * clusters based on Druid table name and some runtime configs.
 */
public interface WebServiceHandlerSelector {
    /**
     * Select which web service to use, based on the request information.
     *
     * @param druidQuery  Druid query we intend to send to the chosen WebService
     * @param request  API Request
     * @param context  Context for the request
     *
     * @return the appropriate WebServiceHandler for this request
     */
    WebServiceHandler select(DruidAggregationQuery<?> druidQuery, DataApiRequest request, RequestContext context);
}
