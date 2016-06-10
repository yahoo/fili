// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.handlers.RequestHandlerUtils;

import com.fasterxml.jackson.databind.ObjectWriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * Response Processor which will perform result set mapping
 */
public abstract class MappingResponseProcessor implements ResponseProcessor {

    protected final DataApiRequest apiRequest;
    protected final Map<String, Object> responseContext;
    protected final MultivaluedMap<String, Object> headers;

    protected final List<ResultSetMapper> mappers;
    protected final ObjectMappersSuite objectMappers;
    protected final ObjectWriter writer;

    public MappingResponseProcessor(
            DataApiRequest apiRequest,
            ObjectMappersSuite objectMappers
    ) {
        this.apiRequest = apiRequest;
        this.mappers = buildResultSetMapperList(apiRequest);
        this.headers = buildHeaderList();
        this.responseContext = new HashMap<>();
        this.objectMappers = objectMappers;
        this.writer = objectMappers.getMapper().writer();
    }

    /**
     * Extract all ResultSetMappers from the api request
     *
     * @param apiRequest  The query api request
     * @return a list of all mappers for this apirequest
     */
    protected static List<ResultSetMapper> buildResultSetMapperList(DataApiRequest apiRequest) {
        return apiRequest.getLogicalMetrics().stream()
                .map(LogicalMetric::getCalculation)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Create headers which belong in the response
     *
     * @return A list of HTTP headers to attach to the response
     */
    protected MultivaluedMap<String, Object> buildHeaderList() {
        return new MultivaluedHashMap<>();
    }

    /**
     * Builds a mapped result set by running through ResultSetMappers
     *
     * @param resultSet  The result set being processed
     *
     * @return a mapped resultSet
     */
    protected ResultSet mapResultSet(ResultSet resultSet) {
        ResultSet mappedResultSet = resultSet;
        for (ResultSetMapper resultSetMapper : getMappers()) {
            mappedResultSet = resultSetMapper.map(mappedResultSet);
        }
        return mappedResultSet;
    }

    public static HttpErrorCallback getStandardError(
            final AsyncResponse asyncResponse,
            final DruidAggregationQuery<?> druidQuery,
            final ObjectWriter writer
    ) {
        return new HttpErrorCallback() {
            @Override
            public void invoke(int statusCode, String reason, String responseBody) {
                Response rsp = RequestHandlerUtils.makeErrorResponse(
                        statusCode, reason, responseBody, druidQuery, writer
                );
                if (RequestLog.isStarted(RESPONSE_WORKFLOW_TIMER)) {
                    RequestLog.stopTiming(RESPONSE_WORKFLOW_TIMER);
                }
                asyncResponse.resume(rsp);
            }
        };
    }

    public static FailureCallback getStandardFailure(
            final AsyncResponse asyncResponse,
            final DruidAggregationQuery<?> druidQuery,
            final ObjectWriter writer
    ) {
        return new FailureCallback() {
            @Override
            public void invoke(Throwable error) {
                Response rsp = RequestHandlerUtils.makeErrorResponse(
                        Status.INTERNAL_SERVER_ERROR,
                        druidQuery,
                        error,
                        writer
                );
                if (RequestLog.isStarted(RESPONSE_WORKFLOW_TIMER)) {
                    RequestLog.stopTiming(RESPONSE_WORKFLOW_TIMER);
                }
                asyncResponse.resume(rsp);
            }
        };
    }

    @Override
    public Map<String, Object> getResponseContext() {
        return responseContext;
    }

    public MultivaluedMap<String, Object> getHeaders() {
        return headers;
    }

    public List<ResultSetMapper> getMappers() {
        return mappers;
    }

    public DataApiRequest getDataApiRequest() {
        return apiRequest;
    }
}
