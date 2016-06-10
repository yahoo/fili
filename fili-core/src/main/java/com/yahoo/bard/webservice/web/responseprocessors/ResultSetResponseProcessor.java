// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import static com.yahoo.bard.webservice.web.handlers.PartialDataRequestHandler.getPartialIntervalsWithDefault;
import static com.yahoo.bard.webservice.web.handlers.VolatileDataRequestHandler.getVolatileIntervalsWithDefault;
import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.PAGINATION_CONTEXT_KEY;
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.PAGINATION_LINKS_CONTEXT_KEY;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.DruidResponseParser;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.table.ZonedSchema;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.PageNotFoundException;
import com.yahoo.bard.webservice.web.Response;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.handlers.RequestHandlerUtils;
import com.yahoo.bard.webservice.web.util.ResponseFormat;

import com.fasterxml.jackson.databind.JsonNode;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collections;
import java.util.Map;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

/**
 * Callback handler for JSON to be processed into result sets
 */
public class ResultSetResponseProcessor extends MappingResponseProcessor implements ResponseProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ResultSetResponseProcessor.class);

    protected final AsyncResponse asyncResponse;
    protected final Granularity granularity;
    protected final DruidResponseParser druidResponseParser;

    public ResultSetResponseProcessor(
            DataApiRequest apiRequest,
            AsyncResponse asyncResponse,
            DruidResponseParser druidResponseParser,
            ObjectMappersSuite objectMappers
    ) {
        super(apiRequest, objectMappers);
        this.granularity = apiRequest.getGranularity();
        this.asyncResponse = asyncResponse;
        this.druidResponseParser = druidResponseParser;
    }

    @Override
    public FailureCallback getFailureCallback(DruidAggregationQuery<?> druidQuery) {
        return getStandardFailure(asyncResponse, druidQuery, writer);
    }

    @Override
    public HttpErrorCallback getErrorCallback(DruidAggregationQuery<?> druidQuery) {
        return getStandardError(asyncResponse, druidQuery, writer);
    }

    @Override
    public void processResponse(JsonNode json, DruidAggregationQuery<?> druidQuery, ResponseContext metadata) {
        try {
            RequestLog.restore(metadata.getRequestLog());
            ResultSet resultSet = buildResultSet(json, druidQuery, metadata.getDateTimeZone());
            resultSet = mapResultSet(resultSet);
            javax.ws.rs.core.Response rsp = buildResponse(resultSet);
            if (RequestLog.isStarted(RESPONSE_WORKFLOW_TIMER)) {
                RequestLog.stopTiming(RESPONSE_WORKFLOW_TIMER);
            }
            asyncResponse.resume(rsp);
        } catch (PageNotFoundException invalidPage) {
            Status errorStatus = invalidPage.getErrorStatus();
            LOG.debug(invalidPage.getLogMessage());
            // If we have a PageNotFoundException, then we do not put the druid query in the error response, because
            // this was a user error, not a system error.
            javax.ws.rs.core.Response rsp = RequestHandlerUtils.makeErrorResponse(errorStatus, invalidPage, writer);
            if (RequestLog.isStarted(RESPONSE_WORKFLOW_TIMER)) {
                RequestLog.stopTiming(RESPONSE_WORKFLOW_TIMER);
            }
            asyncResponse.resume(rsp);
        } catch (Exception e) {
            Status errorStatus = Status.BAD_REQUEST;
            LOG.error("Exception processing druid call in success", e);
            javax.ws.rs.core.Response rsp = RequestHandlerUtils.makeErrorResponse(errorStatus, druidQuery, e, writer);
            if (RequestLog.isStarted(RESPONSE_WORKFLOW_TIMER)) {
                RequestLog.stopTiming(RESPONSE_WORKFLOW_TIMER);
            }
            asyncResponse.resume(rsp);
        }
    }

    protected javax.ws.rs.core.Response buildResponse(ResultSet resultSet) {
        javax.ws.rs.core.Response.ResponseBuilder rspBuilder = createResponseBuilder(resultSet);
        //Headers are a multivalued map, and we want to add each element of each value to the builder.
        getHeaders().entrySet().stream().flatMap(entry -> entry.getValue().stream()
                .peek(value -> rspBuilder.header(entry.getKey(), value))).forEach(ignored -> { });

        return rspBuilder.build();
    }

    protected javax.ws.rs.core.Response.ResponseBuilder createResponseBuilder(ResultSet resultSet) {

        @SuppressWarnings("unchecked")
        Map<String, URI> bodyLinks = (Map<String, URI>) responseContext.get(
                PAGINATION_LINKS_CONTEXT_KEY.getName()
        );
        if (bodyLinks == null) {
            bodyLinks = Collections.emptyMap();
        }
        Pagination pagination = (Pagination) responseContext.get(PAGINATION_CONTEXT_KEY.getName());
        final Response response = new Response(
                resultSet,
                apiRequest,
                getPartialIntervalsWithDefault(responseContext),
                getVolatileIntervalsWithDefault(responseContext),
                bodyLinks,
                pagination,
                objectMappers
        );

        // pass stream handler as response
        javax.ws.rs.core.Response.ResponseBuilder rspBuilder = javax.ws.rs.core.Response.ok(
                response.getResponseStream()
        );

        // Add headers for content type
        // default response format is JSON
        ResponseFormatType responseFormatType = ResponseFormatType.JSON;
        if (apiRequest.getFormat() != null) {
            responseFormatType = apiRequest.getFormat();
        }

        // build response
        switch (responseFormatType) {
            case CSV:
                rspBuilder = rspBuilder
                        .header(HttpHeaders.CONTENT_TYPE, "text/csv; charset=utf-8")
                        .header(
                                HttpHeaders.CONTENT_DISPOSITION,
                                ResponseFormat.getCsvContentDispositionValue(apiRequest)
                        );
                break;
            case JSON:
                // Fall-through: Default is JSON
            default:
                rspBuilder = rspBuilder
                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON + "; charset=utf-8");
                break;
        }

        return rspBuilder;
    }

    /**
     * Build a result set using the api request time grain
     *
     * @param json  The json representing the druid response.
     * @param druidQuery  The druid query being processed
     * @param dateTimeZone  The date time zone for parsing result rows
     *
     * @return The initial result set from the json node.
     */
    public ResultSet buildResultSet(JsonNode json, DruidAggregationQuery<?> druidQuery, DateTimeZone dateTimeZone) {
        ZonedSchema resultSetSchema = new ZonedSchema(granularity, dateTimeZone);

        for (Aggregation aggregation : druidQuery.getAggregations()) {
            MetricColumn.addNewMetricColumn(resultSetSchema, aggregation.getName());
        }

        for (PostAggregation postAggregation : druidQuery.getPostAggregations()) {
            MetricColumn.addNewMetricColumn(resultSetSchema, postAggregation.getName());
        }

        for (Dimension dimension : druidQuery.getDimensions()) {
            DimensionColumn.addNewDimensionColumn(resultSetSchema, dimension);
        }

        return druidResponseParser.parse(json, resultSetSchema, druidQuery.getQueryType());
    }
}
