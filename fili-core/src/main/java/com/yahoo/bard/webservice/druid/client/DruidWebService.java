// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client;

import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.web.handlers.RequestContext;

import org.asynchttpclient.Response;

import java.util.concurrent.Future;

/**
 * Represents the druid web service endpoint.
 */
public interface DruidWebService {

    /**
     * Serializes the provided query and invokes POST on the druid broker.
     *
     * @param context  The context for the Request.
     * @param success  callback for handling successful requests.
     * @param error  callback for handling http errors.
     * @param failure  callback for handling exception failures.
     * @param query  The druid query object to serialize.
     *
     * @return a future response to the post query.
     */
    Future<Response> postDruidQuery(
            RequestContext context,
            SuccessCallback success,
            HttpErrorCallback error,
            FailureCallback failure,
            DruidQuery<?> query
    );

    /**
     * Invokes GET on the druid broker with a callback expecting a JSON Object on success.
     *
     * @param success  callback for handling successful requests.
     * @param error  callback for handling http errors.
     * @param failure  callback for handling exception failures.
     * @param resourcePath  The url suffix for the remote resource. The prefix should be the shared Broker endpoint url.
     *
     * @return a future response to the get request.
     */
    Future<Response> getJsonObject(
            SuccessCallback success,
            HttpErrorCallback error,
            FailureCallback failure,
            String resourcePath
    );

    /**
     * Returns the service configuration object for this web service.
     *
     * @return the service configuration object
     */
    DruidServiceConfig getServiceConfig();

    /**
     * Returns the timeout configured on this web service.
     *
     * @return the query timeout in milliseconds
     */
    Integer getTimeout();
}
