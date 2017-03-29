// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.RESULT_MAPPING_FAILURE;
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.API_METRIC_COLUMN_NAMES;
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.HEADERS;
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.REQUESTED_API_DIMENSION_FIELDS;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.async.ResponseException;
import com.yahoo.bard.webservice.data.DruidResponseParser;
import com.yahoo.bard.webservice.data.HttpResponseMaker;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.data.ResultSetSchema;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.PageNotFoundException;
import com.yahoo.bard.webservice.web.PreResponse;

import com.fasterxml.jackson.databind.JsonNode;

import org.joda.time.DateTimeZone;
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
            ResultSet resultSet = buildResultSet(json, druidQuery, apiRequest.getTimeZone());
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
        } catch (IllegalStateException ise) {
            LOG.error(RESULT_MAPPING_FAILURE.logFormat(ise.getMessage()));
            responseEmitter.onError(new ResponseException(
                    Status.INTERNAL_SERVER_ERROR,
                    druidQuery,
                    new Exception(RESULT_MAPPING_FAILURE.format(ise.getMessage())),
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

    /**
     * Build a result set using the api request time grain.
     *
     * @param json  The json representing the druid response.
     * @param druidQuery  The druid query being processed
     * @param dateTimeZone  The date time zone for parsing result rows
     *
     * @return The initial result set from the json node.
     */
    public ResultSet buildResultSet(JsonNode json, DruidAggregationQuery<?> druidQuery, DateTimeZone dateTimeZone) {

        LinkedHashSet<Column> columns = druidResponseParser.buildSchemaColumns(druidQuery)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        ResultSetSchema resultSetSchema = new ResultSetSchema(granularity, columns);

        return druidResponseParser.parse(json, resultSetSchema, druidQuery.getQueryType(), dateTimeZone);
    }
}
