// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client.impl;

import com.yahoo.bard.webservice.druid.client.DruidServiceConfig;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Request;

import io.netty.handler.codec.http.HttpHeaders;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Used for testing AsyncDruidWebServiceImpl.
 */
public class AsyncDruidWebServiceImplWrapper extends AsyncDruidWebServiceImpl {
    public Request request;

    /**
     * Constructor wrapper.
     *
     * @param serviceConfig Service config
     * @param mapper Mapper
     * @param headersToAppend Headers
     */
    public AsyncDruidWebServiceImplWrapper(
            DruidServiceConfig serviceConfig,
            ObjectMapper mapper,
            Supplier<Map<String, String>> headersToAppend
    ) {
        super(serviceConfig, mapper, headersToAppend);
    }

    /**
     * Capture arguments to test for expected values.
     *
     * @param success  callback for handling successful requests.
     * @param error  callback for handling http errors.
     * @param failure  callback for handling exception failures.
     * @param requestBuilder  The bound request builder for the request to be sent.
     * @param timerName  The name that distinguishes this request as part of a druid query or segment metadata request
     * @param outstanding  The counter that keeps track of the outstanding (in flight) requests for the top level query
     */
    @Override
    protected void sendRequest(
            final SuccessCallback success,
            final HttpErrorCallback error,
            final FailureCallback failure,
            final BoundRequestBuilder requestBuilder,
            final String timerName,
            final AtomicLong outstanding
    ) {
        this.request = requestBuilder.build();
    }

    public HttpHeaders getHeaders() {
        return request.getHeaders();
    }
}
