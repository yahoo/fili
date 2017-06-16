// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client.impl;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DRUID_URL_INVALID;
import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER;
import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.druid.client.DruidServiceConfig;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.druid.model.query.WeightEvaluationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.DruidResponse;
import com.yahoo.bard.webservice.util.FailedFuture;
import com.yahoo.bard.webservice.web.handlers.RequestContext;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.ws.rs.core.Response.Status;

/**
 * Represents the druid web service endpoint.
 */
public class AsyncDruidWebServiceImpl implements DruidWebService {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncDruidWebServiceImpl.class);
    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final boolean DRUID_ETAG_CACHE_ENABLED = SYSTEM_CONFIG.getBooleanProperty(
            SYSTEM_CONFIG.getPackageVariableName("druid_etag_cache_enabled"),
            true
    );

    private final AsyncHttpClient webClient;
    private final ObjectWriter writer;
    private final Meter httpErrorMeter;
    private final Meter exceptionMeter;

    public static final String DRUID_TIMER = "DruidProcessing";
    public static final String DRUID_QUERY_TIMER = DRUID_TIMER + "_Q_";
    public static final String DRUID_QUERY_ALL_TIMER = DRUID_QUERY_TIMER + "All";
    public static final String DRUID_QUERY_MAX_TIMER = DRUID_QUERY_TIMER + "Max";
    public static final String DRUID_WEIGHTED_QUERY_TIMER = DRUID_TIMER + "_W_";
    public static final String DRUID_SEGMENT_METADATA_TIMER = DRUID_TIMER + "_S_0";

    /**
     * The default JSON builder puts only response body in the JSON response.
     */
    public static final Function<Response, JsonNode> DEFAULT_JSON_NODE_BUILDER_STRATEGY =
            new Function<Response, JsonNode>() {

        @Override
        public JsonNode apply(Response response) {
            try {
                return new MappingJsonFactory().createParser(response.getResponseBodyAsStream()).readValueAsTree();
            } catch (IOException ioe) {
                throw new IllegalStateException(ioe);
            }
        }
    };

    private final Supplier<Map<String, String>> headersToAppend;
    private final DruidServiceConfig serviceConfig;

    private final Function<Response, JsonNode> jsonNodeBuilderStrategy;

    /**
     * Friendly non-DI constructor useful for manual tests.
     *
     * @param serviceConfig  Configuration for the Druid Service
     * @param mapper  A shared jackson object mapper resource
     * @deprecated We now require a header supplier parameter.
     *      Use {@link #AsyncDruidWebServiceImpl(DruidServiceConfig, AsyncHttpClient, ObjectMapper, Supplier, Function)}
     */
    @Deprecated
    public AsyncDruidWebServiceImpl(
            DruidServiceConfig serviceConfig,
            ObjectMapper mapper
    ) {
        this(
                serviceConfig,
                initializeWebClient(serviceConfig.getTimeout()),
                mapper,
                HashMap::new,
                DEFAULT_JSON_NODE_BUILDER_STRATEGY
        );
    }

    /**
     * IOC constructor.
     *
     * @param config  the configuration for this druid service
     * @param asyncHttpClient  the HTTP client
     * @param mapper  A shared jackson object mapper resource
     * @deprecated  We now require a header supplier parameter.
     *      Use {@link #AsyncDruidWebServiceImpl(DruidServiceConfig, AsyncHttpClient, ObjectMapper, Supplier, Function)}
     */
    @Deprecated
    public AsyncDruidWebServiceImpl(
            DruidServiceConfig config,
            AsyncHttpClient asyncHttpClient,
            ObjectMapper mapper
    ) {
        this(config, asyncHttpClient, mapper, HashMap::new, DEFAULT_JSON_NODE_BUILDER_STRATEGY);
    }

    /**
     * Friendly non-DI constructor useful for manual tests.
     * <p>
     * This constructor uses default JSON builder, which only uses response body to build the JSON response.
     *
     * @param serviceConfig  Configuration for the Druid Service
     * @param mapper  A shared jackson object mapper resource
     * @param headersToAppend Supplier for map of headers for Druid requests
     */
    public AsyncDruidWebServiceImpl(
            DruidServiceConfig serviceConfig,
            ObjectMapper mapper,
            Supplier<Map<String, String>> headersToAppend
    ) {
        this(
                serviceConfig,
                initializeWebClient(serviceConfig.getTimeout()),
                mapper,
                headersToAppend,
                DEFAULT_JSON_NODE_BUILDER_STRATEGY
        );
    }

    /**
     * Friendly non-DI constructor useful for manual tests.
     *
     * @param serviceConfig  Configuration for the Druid Service
     * @param mapper  A shared jackson object mapper resource
     * @param headersToAppend Supplier for map of headers for Druid requests
     * @param jsonNodeBuilderStrategy A function to build JSON nodes from the response
     */
    public AsyncDruidWebServiceImpl(
            DruidServiceConfig serviceConfig,
            ObjectMapper mapper,
            Supplier<Map<String, String>> headersToAppend,
            Function<Response, JsonNode> jsonNodeBuilderStrategy
    ) {
        this(
                serviceConfig,
                initializeWebClient(serviceConfig.getTimeout()),
                mapper,
                headersToAppend,
                jsonNodeBuilderStrategy
        );
    }

    /**
     * IOC constructor.
     * <p>
     * This constructor uses default JSON builder, which only uses response body to build the JSON response.
     *
     * @param config  the configuration for this druid service
     * @param asyncHttpClient  the HTTP client
     * @param mapper  A shared jackson object mapper resource
     * @param headersToAppend Supplier for map of headers for Druid requests
     */
    public AsyncDruidWebServiceImpl(
            DruidServiceConfig config,
            AsyncHttpClient asyncHttpClient,
            ObjectMapper mapper,
            Supplier<Map<String, String>> headersToAppend
    ) {
        this(config, asyncHttpClient, mapper, headersToAppend, DEFAULT_JSON_NODE_BUILDER_STRATEGY);
    }

    /**
     * IOC constructor.
     *
     * @param config  the configuration for this druid service
     * @param asyncHttpClient  the HTTP client
     * @param mapper  A shared jackson object mapper resource
     * @param headersToAppend Supplier for map of headers for Druid requests
     * @param jsonNodeBuilderStrategy A function to build JSON nodes from the response
     */
    public AsyncDruidWebServiceImpl(
            DruidServiceConfig config,
            AsyncHttpClient asyncHttpClient,
            ObjectMapper mapper,
            Supplier<Map<String, String>> headersToAppend,
            Function<Response, JsonNode> jsonNodeBuilderStrategy
    ) {
        this.serviceConfig = config;

        if (serviceConfig.getUrl() == null) {
            String msg = DRUID_URL_INVALID.format(config.getNameAndUrl());
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        LOG.info("Configured with druid server config: {}", config.toString());
        this.headersToAppend = headersToAppend;
        this.webClient = asyncHttpClient;
        this.writer = mapper.writer();
        this.httpErrorMeter = REGISTRY.meter("druid.errors.http");
        this.exceptionMeter = REGISTRY.meter("druid.errors.exceptions");

        this.jsonNodeBuilderStrategy = jsonNodeBuilderStrategy;
    }

    /**
     * Initialize the client config.
     *
     * @param requestTimeout  Timeout to use for the client configuration.
     *
     * @return the set up client
     */
    private static AsyncHttpClient initializeWebClient(int requestTimeout) {

        LOG.debug("Druid request timeout: {}ms", requestTimeout);

        // Build the configuration
        AsyncHttpClientConfig config = new DefaultAsyncHttpClientConfig.Builder()
                .setReadTimeout(requestTimeout)
                .setRequestTimeout(requestTimeout)
                .setConnectTimeout(requestTimeout)
                .setConnectionTtl(requestTimeout)
                .setPooledConnectionIdleTimeout(requestTimeout)
                .setFollowRedirect(true)
                .build();

        return new DefaultAsyncHttpClient(config);
    }


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
                        String druidQueryId = response.getHeader("X-Druid-Query-Id");
                        Status status = Status.fromStatusCode(response.getStatusCode());
                        logRequest(logCtx, timerName, outstanding, druidQueryId, status);

                        if (hasError(status)) {
                            markError(status, response, druidQueryId, error);
                        } else {
                            try {
                                success.invoke(jsonNodeBuilderStrategy.apply(response));
                            } catch (RuntimeException e) {
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

    @Override
    public Future<Response> getJsonObject(
            SuccessCallback success,
            HttpErrorCallback error,
            FailureCallback failure,
            String resourcePath
    ) {
        String url = String.format("%s%s", serviceConfig.getUrl(), resourcePath);

        BoundRequestBuilder requestBuilder = webClient.prepareGet(url);
        headersToAppend.get().forEach(requestBuilder::addHeader);

        return sendRequest(
                success,
                error,
                failure,
                requestBuilder,
                DRUID_SEGMENT_METADATA_TIMER,
                new AtomicLong(1)
        );
    }

    @Override
    public Future<Response> postDruidQuery(
            RequestContext context,
            SuccessCallback success,
            HttpErrorCallback error,
            FailureCallback failure,
            DruidQuery<?> druidQuery
    ) {
        long seqNum = druidQuery.getContext().getSequenceNumber();
        String entityBody;
        RequestLog.startTiming("DruidQuerySerializationSeq" + seqNum);
        try {
            entityBody = writer.writeValueAsString(druidQuery);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        } finally {
            RequestLog.stopTiming("DruidQuerySerializationSeq" + seqNum);
        }

        long totalQueries = druidQuery.getContext().getNumberOfQueries();
        String format = String.format("%%0%dd", String.valueOf(totalQueries).length());
        String timerName;
        AtomicLong outstanding;

        if (!(druidQuery instanceof WeightEvaluationQuery)) {
            if (context.getNumberOfOutgoing().decrementAndGet() == 0) {
                RequestLog.stopTiming(REQUEST_WORKFLOW_TIMER);
            }
            outstanding = context.getNumberOfIncoming();
            timerName = DRUID_QUERY_TIMER + String.format(format, seqNum);
        } else {
            outstanding = new AtomicLong(0);
            timerName = DRUID_WEIGHTED_QUERY_TIMER + String.format(format, seqNum);
        }

        BoundRequestBuilder requestBuilder = webClient.preparePost(serviceConfig.getUrl())
                .setBody(entityBody)
                .addHeader("Content-Type", "application/json; charset=UTF-8");

        headersToAppend.get().forEach(requestBuilder::addHeader);

        LOG.debug("druid json request: {}", entityBody);
        return sendRequest(
                success,
                error,
                failure,
                requestBuilder,
                timerName,
                outstanding
        );
    }

    @Override
    public Integer getTimeout() {
        return serviceConfig.getTimeout();
    }

    @Override
    public DruidServiceConfig getServiceConfig() {
        return serviceConfig;
    }

    protected Meter getHttpErrorMeter() {
        return httpErrorMeter;
    }

    protected Meter getExceptionMeter() {
        return exceptionMeter;
    }

    protected DruidServiceConfig getDruidServiceConfig() {
        return serviceConfig;
    }

    /**
     * <ol>
     *     <li>Logs request using RequestLog,</li>
     *     <li>restores the serialized request log info back into the RequestLog object,</li>
     *     <li>stops timer that distinguishes this request as part of a druid query or segment metadata request,</li>
     *     <li>starts response workflow timer,</li>
     *     <li>decrements counter that keeps track of the outstanding (in flight) requests for the top level query,</li>
     *     <li>and logs Druid response.</li>
     * </ol>
     *
     * @param logCtx  The snapshot of the request log of the current thread
     * @param timerName  The name that distinguishes this request as part of a druid query or segment metadata request
     * @param outstanding  The counter that keeps track of the outstanding (in flight) requests for the top level query
     * @param druidQueryId  The Druid query ID
     * @param status  The response status
     */
    private void logRequest(
            RequestLog logCtx,
            String timerName,
            AtomicLong outstanding,
            String druidQueryId,
            Status status
    ) {
        RequestLog.restore(logCtx);
        RequestLog.stopTiming(timerName);
        if (outstanding.decrementAndGet() == 0) {
            RequestLog.startTiming(RESPONSE_WORKFLOW_TIMER);
        }
        RequestLog.record(new DruidResponse(druidQueryId));

        LOG.debug(
                "druid {} response code: {} {} and druid query id: {}",
                getServiceConfig().getNameAndUrl(),
                status.getStatusCode(),
                status,
                druidQueryId
        );
    }

    /**
     * Return true if response status code indicates an error.
     * <p>
     * If etag cache is enabled, i.e. DRUID_ETAG_CACHE_ENABLED is set to true, no error on 200 OK and 304 NOT-MODIFIED.
     * Otherwise, no error only on 200 OK.
     *
     * @param status  The Status object that contains status code to be checked
     *
     * @return true if the status code indicates an error
     */
    protected boolean hasError(Status status) {
        return DRUID_ETAG_CACHE_ENABLED
                ? status != Status.OK && status != Status.NOT_MODIFIED
                : status != Status.OK;
    }

    /**
     * Count and log error, then send error response.
     *
     * @param status  The response status
     * @param response  The druid response
     * @param druidQueryId  The Druid query ID
     * @param error  callback for handling http errors.
     */
    private void markError(Status status, Response response, String druidQueryId, HttpErrorCallback error) {
        getHttpErrorMeter().mark();
        LOG.debug(
                "druid {} error: {} {} {} and druid query id: {}",
                getServiceConfig().getNameAndUrl(),
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
    }
}
