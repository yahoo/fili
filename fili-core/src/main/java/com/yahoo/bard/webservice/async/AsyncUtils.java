// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async;

import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.table.Schema;
import com.yahoo.bard.webservice.web.PreResponse;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys;

import java.util.Collections;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Contains a collection of utility functions for asynchronous processing.
 */
public class AsyncUtils {

    /**
     * Given a String containing the metadata of an asynchronous job, returns a Response with that metadata as payload.
     *
     * @param jobMetadata  The metadata to ship back to the user
     *
     * @return A Response with the specified metadata as payload
     */
    public static Response makeJobMetadataResponse(String jobMetadata) {
        return Response.status(Response.Status.ACCEPTED)
                .entity(jobMetadata)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON + "; charset=utf-8")
                .build();
    }

    /**
     * Builds the PreResponse that stores the error message.
     * <p>
     * A PreResponse containing just an error has an empty ResultSet, a Schema that uses the AllGranularity as a
     * placeholder granularity, and contains in the ResponseContext the status code for the error response, and
     * an error message.
     *
     * @param throwable  The error that needs to be stored
     *
     * @return A PreResponse containing the error information that should be sent to the user
     */
    public static PreResponse buildErrorPreResponse(Throwable throwable) {
        ResponseContext responseContext = new ResponseContext(Collections.emptyMap());
        if (throwable instanceof ResponseException) {
            ResponseException responseException = (ResponseException) throwable;
            responseContext.put(ResponseContextKeys.STATUS.getName(), responseException.getStatusCode());
            responseContext.put(ResponseContextKeys.ERROR_MESSAGE.getName(), responseException.getReason());
        } else {
            responseContext.put(
                    ResponseContextKeys.STATUS.getName(),
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()
            );
            responseContext.put(ResponseContextKeys.ERROR_MESSAGE.getName(), throwable.getMessage());
        }

        return new PreResponse(
                new ResultSet(Collections.emptyList(), new Schema(AllGranularity.INSTANCE)),
                responseContext
        );
    }
}
