// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.models.druid.client.impl;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.druid.client.DruidServiceConfig;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.druid.model.query.WeightEvaluationQuery;
import com.yahoo.bard.webservice.util.CompletedFuture;
import com.yahoo.bard.webservice.util.JsonSlurper;
import com.yahoo.bard.webservice.web.handlers.RequestContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.asynchttpclient.Response;
import org.glassfish.jersey.internal.util.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

import javax.ws.rs.core.Response.Status;

/**
 * Test druid web service acts as a proxy for a real web service, accepting requests and saving them and providing
 * responses.
 */
public class TestDruidWebService implements DruidWebService {
    private static final Logger LOG = LoggerFactory.getLogger(TestDruidWebService.class);

    public static String DEFAULT_NAME = "unnamed";

    private static ObjectMapper mapper = new ObjectMappersSuite().getMapper();

    private static ObjectWriter writer = mapper.writer();

    public Producer<String> jsonResponse = () -> "[]";
    public String timeBoundaryResponse = "[]";
    public String segmentMetadataResponse = "[]";
    public int statusCode = 200;
    public String statusName = "";
    public String reasonPhrase = "";
    public Throwable throwable = null;
    public DruidQuery<?> lastQuery = null;
    public String lastUrl = null;
    public String jsonQuery = "";
    public Integer timeout = null;
    public String weightResponse = "[ { \"event\" : { \"count\" : 19 } } ]";
    public DruidServiceConfig serviceConfig;

    /**
     * Constructor.
     *
     * @param serviceConfig  Config to use when building the test druid web service
     */
    public TestDruidWebService(DruidServiceConfig serviceConfig) {
        this.serviceConfig = serviceConfig;
        this.timeout = serviceConfig.getTimeout();
    }

    /**
     * Constructor.
     *
     * @param name  Name of the webservice
     */
    public TestDruidWebService(String name) {
        this(new DruidServiceConfig(name, null, null, null));
    }

    /**
     * Constructor.
     * <p>
     * Uses the default name.
     */
    public TestDruidWebService() {
        this(new DruidServiceConfig(DEFAULT_NAME, null, null, null));
    }

    /**
     * If TestDruidWebService#throwable is set, invokes the failure callback.  Otherwise invokes success or failure
     * dependent on whether TestDruidWebService#statusCode equals 200.
     * Note that since this doesn't send requests to druid all the responses will be null or {@link CompletedFuture}.
     */
    @Override
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public Future<Response> postDruidQuery(
            RequestContext context,
            SuccessCallback success,
            HttpErrorCallback error,
            FailureCallback failure,
            DruidQuery<?> query
    ) {
        LOG.info("Invoking test druid webservice: {}", this);

        // Store the most recent query sent through
        lastQuery = query;

        // Convert the query to json
        try {
            jsonQuery = writer.withDefaultPrettyPrinter().writeValueAsString(query);
        } catch (JsonProcessingException ignored) {
            // Ignore
        }

        // Invoke failure callback if we have a throwable to give it
        if (throwable != null) {
            failure.invoke(throwable);
            return CompletedFuture.throwing(throwable);
        }

        if (lastQuery.getQueryType() instanceof DefaultQueryType) {
            // For known response types, create a default response provider

            // Set the response to use based on the type of the query we're processing
            DefaultQueryType defaultQueryType = (DefaultQueryType) lastQuery.getQueryType();
            switch (defaultQueryType) {
                case GROUP_BY:
                case TOP_N:
                case TIMESERIES:
                case LOOKBACK:
                    // default response is groupBy response
                    break;
                case SEGMENT_METADATA:
                    jsonResponse = () -> segmentMetadataResponse;
                    break;
                case TIME_BOUNDARY:
                    jsonResponse = () -> timeBoundaryResponse;
                    break;
                default:
                    throw new IllegalArgumentException("Illegal query type : " + lastQuery.getQueryType());
            }

        } else {
            // Otherwise extended query types will have to set up their own responses
        }

        try {
            if (query instanceof WeightEvaluationQuery) {
                success.invoke(mapper.readTree(weightResponse));
            } else if (statusCode == 200) {
                success.invoke(mapper.readTree(jsonResponse.call()));
            } else {
                error.invoke(statusCode, reasonPhrase, jsonResponse.call());
            }
        } catch (IOException e) {
            failure.invoke(e);
            return CompletedFuture.throwing(e);
        }

        return ConcurrentUtils.constantFuture(null);
    }

    /**
     * Allows test to set the Druid failure response.
     *
     * @param json  inbound test
     */
    public void setFailure(String json) {
        @SuppressWarnings("unchecked")
        Map<String, Object> failure = (Map<String, Object>) new JsonSlurper().parseText(json);

        //extract only status code and description from expected response string
        statusCode = (int) failure.get("status");
        reasonPhrase = (String) failure.get("reason");
        jsonResponse = () -> (String) failure.get("description");
    }


    /**
     * Allows test to set the Druid failure response.
     *
     * @param status  failure status
     * @param reason  failure reason string
     * @param response  json response
     */
    public void setFailure(Status status, String reason, String response) {
        setFailure(status.getStatusCode(), status.name(), reason, response);
    }

    /**
     * Allows test to set the Druid failure response.
     *
     * @param statusCode  failure status
     * @param statusName  Name of the status
     * @param reason  failure reason string
     * @param response  json response
     */
    public void setFailure(int statusCode, String statusName, String reason, String response) {
        //extract only status code and description from expected response string
        this.statusCode = statusCode;
        this.statusName = statusName;
        reasonPhrase = reason;
        jsonResponse = () -> response;
    }

    @Override
    public Integer getTimeout() {
        return serviceConfig.getTimeout();
    }

    @Override
    public DruidServiceConfig getServiceConfig() {
        return serviceConfig;
    }

    @Override
    public String toString() {
        return " name: " + serviceConfig.getNameAndUrl() + " hash code: " + hashCode();
    }

    @Override
    public Future<Response> getJsonObject(
            SuccessCallback success,
            HttpErrorCallback error,
            FailureCallback failure,
            String resourcePath
    ) {
        LOG.info("Invoking test druid webservice: {}", this);

        // Store the most recent query sent through
        lastUrl = resourcePath;

        // Invoke failure callback if we have a throwable to give it
        if (throwable != null) {
            failure.invoke(throwable);
            return CompletedFuture.throwing(throwable);
        }

         try {
             if (statusCode == 200) {
                success.invoke(mapper.readTree(jsonResponse.call()));
            } else {
                error.invoke(statusCode, reasonPhrase, jsonResponse.call());
            }
        } catch (IOException e) {
            failure.invoke(e);
             return CompletedFuture.throwing(throwable);
        }

        return ConcurrentUtils.constantFuture(null);
    }
}
