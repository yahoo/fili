// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client.impl;

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.druid.client.DruidServiceConfig;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.DruidResponse;
import com.yahoo.bard.webservice.util.FailedFuture;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Represents the druid web service endpoint for partial data V2.
 */
public class AsyncDruidWebServiceImplV2 extends AsyncDruidWebServiceImpl {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncDruidWebServiceImplV2.class);

    /**
     * IOC constructor.
     *
     * @param config  the configuration for this druid service
     * @param mapper  A shared jackson object mapper resource
     * @param headersToAppend Supplier for map of headers for Druid requests
     */
    public AsyncDruidWebServiceImplV2(
            DruidServiceConfig config,
            ObjectMapper mapper,
            Supplier<Map<String, String>> headersToAppend
    ) {
        super(config, mapper, headersToAppend);
    }

    @Override
    /**
     * Serializes the provided query and invokes a request on the druid broker.
     *
     * @param success  callback for handling successful requests.
     * @param error  callback for handling http errors.
     * @param failure  callback for handling exception failures.
     * @param requestBuilder  The bound request builder for the request to be sent.
     * @param timerName  The name that distinguishes this request as part of a druid query or segment metadata request
     * @param outstanding  The counter that keeps track of the outstanding (in flight) requests for the top level query
     *
     * @return a future response for the query being sent
     */
    protected Future<Response> sendRequest(
            final SuccessCallback success,
            final HttpErrorCallback error,
            final FailureCallback failure,
            final BoundRequestBuilder requestBuilder,
            final String timerName,
            final AtomicLong outstanding
    ) {
        RequestLog.startTiming(timerName);
        final RequestLog logCtx = RequestLog.dump();
        try {
            return requestBuilder.execute(
                    new AsyncCompletionHandler<Response>() {
                        @Override
                        public Response onCompleted(Response response) {
                            RequestLog.restore(logCtx);
                            RequestLog.stopTiming(timerName);
                            if (outstanding.decrementAndGet() == 0) {
                                RequestLog.startTiming(RESPONSE_WORKFLOW_TIMER);
                            }
                            String druidQueryId = response.getHeader("X-Druid-Query-Id");
                            RequestLog.record(new DruidResponse(druidQueryId));

                            javax.ws.rs.core.Response.Status status = javax.ws.rs.core.Response.Status.fromStatusCode(
                                    response.getStatusCode()
                            );
                            LOG.debug(
                                    "druid {} response code: {} {} and druid query id: {}",
                                    serviceConfig.getNameAndUrl(),
                                    status.getStatusCode(),
                                    status,
                                    druidQueryId
                            );

                            if (status != javax.ws.rs.core.Response.Status.OK) {
                                httpErrorMeter.mark();
                                LOG.debug(
                                        "druid {} error: {} {} {} and druid query id: {}",
                                        serviceConfig.getNameAndUrl(),
                                        status.getStatusCode(),
                                        status.getReasonPhrase(),
                                        response.getResponseBody(),
                                        druidQueryId
                                );

                                error.invoke(
                                        status.getStatusCode(),
                                        status.getReasonPhrase(),
                                        response.getResponseBody()
                                );
                            } else {
                                MappingJsonFactory jsonFactory = new MappingJsonFactory();
                                try {
                                    JsonNode rootNode;
                                    try (
                                            InputStream responseStream = new SequenceInputStream(
                                                    response.getResponseBodyAsStream(),
                                                    new ByteArrayInputStream(
                                                            response.getHeader("X-Druid-Response-Context")
                                                                    .getBytes(StandardCharsets.UTF_8)
                                                    )
                                            );
                                            JsonParser jsonParser = jsonFactory.createParser(responseStream)
                                    ) {
                                        rootNode = jsonParser.readValueAsTree();
                                    }
                                    success.invoke(rootNode);
                                } catch (RuntimeException | IOException e) {
                                    failure.invoke(e);
                                }
                            }

                            // we consumed this response, so pass null to any chains
                            return null;
                        }

                        @Override
                        public void onThrowable(Throwable t) {
                            RequestLog.restore(logCtx);
                            RequestLog.stopTiming(timerName);
                            if (outstanding.decrementAndGet() == 0) {
                                RequestLog.startTiming(RESPONSE_WORKFLOW_TIMER);
                            }
                            exceptionMeter.mark();
                            LOG.error("druid {} request failed:", serviceConfig.getNameAndUrl(), t);
                            failure.invoke(t);
                        }
                    });
        } catch (RuntimeException t) {
            RequestLog.restore(logCtx);
            RequestLog.stopTiming(timerName);
            if (outstanding.decrementAndGet() == 0) {
                RequestLog.startTiming(RESPONSE_WORKFLOW_TIMER);
            }
            LOG.error("druid {} http request failed: ", serviceConfig.getNameAndUrl(), t);
            failure.invoke(t);
            return new FailedFuture<>(t);
        }
    }
}
