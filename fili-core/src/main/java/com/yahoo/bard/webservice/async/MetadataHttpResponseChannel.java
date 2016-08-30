// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async;

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.handlers.RequestHandlerUtils;

import com.fasterxml.jackson.databind.ObjectWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observer;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

/**
 * Handles sending job information to the user.
 */
public class MetadataHttpResponseChannel implements Observer<String> {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataHttpResponseChannel.class);

    private final AsyncResponse asyncResponse;
    private final ObjectWriter writer;

    /**
     * Builds a channel for sending metadata requests back to the user.
     *
     * @param asyncResponse  The asynchronous response via which we send responses back to the user
     * @param writer  JSON serializer used to serialize error messages
     */
    public MetadataHttpResponseChannel(AsyncResponse asyncResponse, ObjectWriter writer) {
        this.asyncResponse = asyncResponse;
        this.writer = writer;
    }

    @Override
    public void onCompleted() {
        //Intentionally left blank.
    }

    @Override
    public void onError(Throwable error) {
        if (error instanceof ResponseException) {
            ResponseException responseException = (ResponseException) error;
            send(buildErrorResponse(responseException), asyncResponse);
        } else {
            LOG.error(ErrorMessageFormat.FAILED_TO_PUBLISH_ERROR.getMessageFormat(), error);
            throw new RuntimeException(ErrorMessageFormat.FAILED_TO_PUBLISH_ERROR.getMessageFormat(), error);
        }
    }

    @Override
    public void onNext(String jobView) {
        send(AsyncUtils.makeJobMetadataResponse(jobView), asyncResponse);
    }

    /**
     * Sends the response back to the user.
     *
     * @param response  The response to send to the user
     * @param asyncResponse  The channel over which to send the response
     */
    private void send(Response response, AsyncResponse asyncResponse) {
        if (RequestLog.isStarted(RESPONSE_WORKFLOW_TIMER)) {
            RequestLog.stopTiming(RESPONSE_WORKFLOW_TIMER);
        }
        asyncResponse.resume(response);
    }

    /**
     * Prepare Response object from error details with reason and description.
     *
     * @param responseException  The error that needs to be transmitted to the user
     *
     * @return Publishable Response object
     */
    private javax.ws.rs.core.Response buildErrorResponse(ResponseException responseException) {
        return RequestHandlerUtils.makeErrorResponse(
                responseException.getStatusCode(),
                responseException.getReason(),
                responseException.getDescription(),
                responseException.getDruidQuery(),
                writer
        );
    }
}
