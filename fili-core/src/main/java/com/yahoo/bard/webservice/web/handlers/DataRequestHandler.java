// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers;

import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;

/**
 * Data Request Handlers are links in a processing chain for Data requests. At each
 * stage the handler is expected to do one of:
 * <ul>
 *    <li> write a response using a Response Processor
 *    <li> delegate response processing synchronously to another Data Request Handler
 *    <li> delegate the response asynchronously to response processing or another Data Request Handler
 * </ul>
 */
public interface DataRequestHandler {

    /**
     * Handle the response, passing the request down the chain as necessary.
     *
     * @param context  The context for the Request
     * @param request  The Api Request Object
     * @param druidQuery  The druid query
     * @param response  The Async response
     *
     * @return True if the async response has been processed or passed to a future for processing
     */
    boolean handleRequest(
            RequestContext context,
            final DataApiRequest request,
            final DruidAggregationQuery<?> druidQuery,
            final ResponseProcessor response
    );
}
