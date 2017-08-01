// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.async.ResponseException;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.web.ApiRequest;
import com.yahoo.bard.webservice.web.PreResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observer;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

/**
 * Converts preResponse/errorResponse into HTTP Responses, and ships them immediately to the client.
 */
public class HttpResponseChannel implements Observer<PreResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpResponseChannel.class);

    private final AsyncResponse asyncResponse;
    private final HttpResponseMaker httpResponseMaker;
    private final ApiRequest apiRequest;

    /**
     * Constructor.
     *
     * @param asyncResponse  An async response that we can use to respond asynchronously
     * @param apiRequest  Api request object with all the associated info with it
     * @param httpResponseMaker  Helper class instance to prepare the response object
     *
     */
    public HttpResponseChannel(
            AsyncResponse asyncResponse,
            ApiRequest apiRequest,
            HttpResponseMaker httpResponseMaker
    ) {
        this.asyncResponse = asyncResponse;
        this.httpResponseMaker = httpResponseMaker;
        this.apiRequest = apiRequest;
    }

    @Override
    public void onCompleted() {
        /* Keeping this method empty on purpose. This method will be invoked only when there is a message to publish.
         * In this contract, we always receive a message to publish to an end user.
         */
    }

    /**
     * Method to handle an error case from its observables. Default argument for this method is Throwable.
     * Customized this argument as Exception which contains the attributes to prepare the error Response.
     * If the throwable is not the type of `ResponseException`, then it will be re-thrown as RuntimeException.
     *
     * @param responseException  An object contains error response attributes wrapped along with an Exception object
     */
    @Override
    public void onError(Throwable responseException) {
        if (responseException instanceof ResponseException) {
            ResponseException resException = (ResponseException) responseException;
            publishResponse(
                    httpResponseMaker.buildErrorResponse(
                            resException.getStatusCode(),
                            resException.getReason(),
                            resException.getDescription(),
                            resException.getDruidQuery()
                    )
            );
        } else {
            LOG.error("Failed to publish error Response", responseException);
            throw new RuntimeException("Failed to publish error Response", responseException);
        }
    }

    @Override
    public void onNext(PreResponse preResponse) {
        publishResponse(httpResponseMaker.buildResponse(preResponse, apiRequest));
    }

    /**
     * Publish final response to user.
     *
     * @param response  The Response to send back to the user
     */
    private void publishResponse(Response response) {
        if (RequestLog.isRunning(RESPONSE_WORKFLOW_TIMER)) {
            RequestLog.stopTiming(RESPONSE_WORKFLOW_TIMER);
        }
        asyncResponse.resume(response);
    }
}
