// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.API_METRIC_COLUMN_NAMES;
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.HEADERS;
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.REQUESTED_API_DIMENSION_FIELDS;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.DruidResponseParser;
import com.yahoo.bard.webservice.data.HttpResponseMaker;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.async.ResponseException;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.table.ZonedSchema;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.PageNotFoundException;
import com.yahoo.bard.webservice.web.PreResponse;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.subjects.Subject;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response.Status;

/**
 * Callback handler for JSON to be processed into result sets.
 */
public class ResultSetResponseProcessor extends MappingResponseProcessor implements ResponseProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ResultSetResponseProcessor.class);

    protected final Subject<PreResponse, PreResponse> responseEmitter;
    protected final Granularity granularity;
    protected final DruidResponseParser druidResponseParser;
    protected HttpResponseMaker httpResponseMaker;

    /**
     * Constructor.
     *
     * @param apiRequest  The request for which the response is being processed
     * @param responseEmitter  The response channel to which the response will be sent
     * @param druidResponseParser  The parser for the Druid response
     * @param objectMappers  Jackson mappers to use for processing JSON
     * @param httpResponseMaker  Helper to make the HTTP response
     */
    public ResultSetResponseProcessor(
            DataApiRequest apiRequest,
            Subject<PreResponse, PreResponse> responseEmitter,
            DruidResponseParser druidResponseParser,
            ObjectMappersSuite objectMappers,
            HttpResponseMaker httpResponseMaker
    ) {
        super(apiRequest, objectMappers);
        this.granularity = apiRequest.getGranularity();
        this.responseEmitter = responseEmitter;
        this.druidResponseParser = druidResponseParser;
        this.httpResponseMaker = httpResponseMaker;
    }

    @Override
    public FailureCallback getFailureCallback(DruidAggregationQuery<?> druidQuery) {
        return getStandardFailure(responseEmitter, druidQuery);
    }

    @Override
    public HttpErrorCallback getErrorCallback(DruidAggregationQuery<?> druidQuery) {
        return getStandardError(responseEmitter, druidQuery);
    }

    @Override
    public void processResponse(JsonNode json, DruidAggregationQuery<?> druidQuery, LoggingContext metadata) {
        try {
            RequestLog.restore(metadata.getRequestLog());
            ZonedSchema resultSetSchema = druidResponseParser.buildSchema(
                            druidQuery, granularity, apiRequest.getTimeZone());
            ResultSet resultSet =  druidResponseParser.parse(json, resultSetSchema, druidQuery.getQueryType());

            resultSet = mapResultSet(resultSet);

            LinkedHashSet<String> apiMetricColumnNames = apiRequest.getLogicalMetrics().stream()
                    .map(LogicalMetric::getName)
                    .collect(Collectors.toCollection(LinkedHashSet::new));

            LinkedHashMap<String, HashSet<DimensionField>> requestedApiDimensionFields = apiRequest.getDimensionFields()
                    .entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> e.getKey().getApiName(),
                            Map.Entry::getValue,
                            (fieldWithSameKey1, fieldWithSameKey2) -> fieldWithSameKey1,
                            LinkedHashMap::new
                    ));

            responseContext.put(API_METRIC_COLUMN_NAMES.getName(), apiMetricColumnNames);
            responseContext.put(HEADERS.getName(), headers);
            responseContext.put(REQUESTED_API_DIMENSION_FIELDS.getName(), requestedApiDimensionFields);

            responseEmitter.onNext(new PreResponse(resultSet, responseContext));
            responseEmitter.onCompleted();
        } catch (PageNotFoundException invalidPage) {
            LOG.debug(invalidPage.getLogMessage());
            responseEmitter.onError(new ResponseException(
                    invalidPage.getErrorStatus(),
                    druidQuery,
                    invalidPage,
                    getObjectMappers().getMapper().writer()
            ));
        } catch (Exception exception) {
            LOG.error("Exception processing druid call in success", exception);
            responseEmitter.onError(new ResponseException(
                    Status.INTERNAL_SERVER_ERROR,
                    druidQuery,
                    exception,
                    getObjectMappers().getMapper().writer()
            ));
        }
    }
}
