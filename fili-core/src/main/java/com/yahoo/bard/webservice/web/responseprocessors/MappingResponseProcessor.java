// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.async.ResponseException;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.subjects.Subject;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;

/**
 * Response Processor which will perform result set mapping.
 */
public abstract class MappingResponseProcessor implements ResponseProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(MappingResponseProcessor.class);

    protected final DataApiRequest apiRequest;
    protected final ResponseContext responseContext;
    protected final MultivaluedHashMap<String, Serializable> headers;

    protected final List<ResultSetMapper> mappers;
    protected final ObjectMappersSuite objectMappers;

    /**
     * Constructor.
     *
     * @param apiRequest  The request for which the response is being processed
     * @param objectMappers  Jackson mappers to use for processing JSON
     */
    public MappingResponseProcessor(DataApiRequest apiRequest, ObjectMappersSuite objectMappers) {
        this.apiRequest = apiRequest;
        this.mappers = buildResultSetMapperList(apiRequest);
        this.headers = buildHeaderList();
        this.responseContext = new ResponseContext(apiRequest.getDimensionFields());
        this.objectMappers = objectMappers;
    }

    /**
     * Extract all ResultSetMappers from the api request.
     *
     * @param apiRequest  The query api request
     *
     * @return a list of all mappers for this apirequest
     */
    protected List<ResultSetMapper> buildResultSetMapperList(DataApiRequest apiRequest) {
        return apiRequest.getLogicalMetrics().stream()
                .map(LogicalMetric::getCalculation)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Create headers which belong in the response.
     *
     * @return A list of HTTP headers to attach to the response
     */
    protected MultivaluedHashMap<String, Serializable> buildHeaderList() {
        return new MultivaluedHashMap<>();
    }

    /**
     * Builds a mapped result set by running through ResultSetMappers.
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

    /**
     * Get the standard error callback.
     *
     * @param responseEmitter  Channel to send the error response to
     * @param druidQuery  Query for which we got an error
     *
     * @return the standard error callback
     */
    public HttpErrorCallback getStandardError(
            final Subject responseEmitter,
            final DruidAggregationQuery<?> druidQuery
    ) {
        return new HttpErrorCallback() {
            @Override
            public void invoke(int statusCode, String reason, String responseBody) {
                LOG.error(ErrorMessageFormat.ERROR_FROM_DRUID.logFormat(responseBody, statusCode, reason, druidQuery));
                responseEmitter.onError(new ResponseException(
                        statusCode,
                        reason,
                        responseBody,
                        druidQuery,
                        null,
                        getObjectMappers().getMapper().writer()
                ));
            }
        };
    }

    /**
     * Get the standard failure callback.
     *
     * @param responseEmitter  Channel to send the response to
     * @param druidQuery  Query for which we got a failure
     *
     * @return the standard failure callback
     */
    public FailureCallback getStandardFailure(
            final Subject responseEmitter,
            final DruidAggregationQuery<?> druidQuery
    ) {
        return new FailureCallback() {
            @Override
            public void invoke(Throwable error) {
                LOG.error(ErrorMessageFormat.FAILED_TO_SEND_QUERY_TO_DRUID.logFormat(druidQuery), error);
                responseEmitter.onError(new ResponseException(
                        Status.INTERNAL_SERVER_ERROR,
                        druidQuery,
                        error,
                        objectMappers.getMapper().writer()
                ));
            }
        };
    }

    @Override
    public ResponseContext getResponseContext() {
        return responseContext;
    }

    public MultivaluedMap<String, Serializable> getHeaders() {
        return headers;
    }

    public List<ResultSetMapper> getMappers() {
        return mappers;
    }

    public DataApiRequest getDataApiRequest() {
        return apiRequest;
    }

    protected ObjectMappersSuite getObjectMappers() {
        return objectMappers;
    }
}
