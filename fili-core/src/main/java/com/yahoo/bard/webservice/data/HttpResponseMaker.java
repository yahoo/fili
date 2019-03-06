// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import static com.yahoo.bard.webservice.web.handlers.PartialDataRequestHandler.getPartialIntervalsWithDefault;
import static com.yahoo.bard.webservice.web.handlers.VolatileDataRequestHandler.getVolatileIntervalsWithDefault;
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.API_METRIC_COLUMN_NAMES;
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.HEADERS;
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.PAGINATION_CONTEXT_KEY;
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.PAGINATION_LINKS_CONTEXT_KEY;
import static com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys.REQUESTED_API_DIMENSION_FIELDS;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.DefaultResponseFormatType;
import com.yahoo.bard.webservice.web.PreResponse;
import com.yahoo.bard.webservice.web.ResponseData;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.ResponseWriter;
import com.yahoo.bard.webservice.web.apirequest.ApiRequest;
import com.yahoo.bard.webservice.web.handlers.RequestHandlerUtils;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext;
import com.yahoo.bard.webservice.web.util.ResponseUtils;

import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;

/**
 * Translates a PreResponse into an HTTP Response containing the results of a query.
 */
@Singleton
public class HttpResponseMaker {

    protected final ObjectMappersSuite objectMappers;
    protected final DimensionDictionary dimensionDictionary;
    protected final ResponseWriter responseWriter;
    protected final ResponseUtils responseUtils;

    /**
     * Class constructor.
     *
     * @param objectMappers  Mappers object for serialization
     * @param dimensionDictionary  The dimension dictionary from which to look up dimensions by name
     * @param responseWriter  Serializer which takes responseData and apiRequest, outputs formatted data stream.
     * @param responseUtils A class providing utility methods for processing response headers.
     */
    @Inject
    public HttpResponseMaker(
            ObjectMappersSuite objectMappers,
            DimensionDictionary dimensionDictionary,
            ResponseWriter responseWriter,
            ResponseUtils responseUtils
    ) {
        this.objectMappers = objectMappers;
        this.dimensionDictionary = dimensionDictionary;
        this.responseWriter = responseWriter;
        this.responseUtils = responseUtils;
    }

    /**
     * Class constructor.
     *
     * @param objectMappers  Mappers object for serialization
     * @param dimensionDictionary  The dimension dictionary from which to look up dimensions by name
     * @param responseWriter  Serializer which takes responseData and apiRequest, outputs formatted data stream.
     */
    public HttpResponseMaker(
            ObjectMappersSuite objectMappers,
            DimensionDictionary dimensionDictionary,
            ResponseWriter responseWriter
    ) {
        this(objectMappers, dimensionDictionary, responseWriter, new ResponseUtils());
    }

    /**
     * Build complete response.
     *
     * @param preResponse  PreResponse object which contains result set, response context and headers
     * @param apiRequest  ApiRequest object which contains request related information
     * @param containerRequestContext The container for jersey request processing objects
     *
     * @return Completely built response with headers and result set
     */
    public javax.ws.rs.core.Response buildResponse(
            PreResponse preResponse,
            ApiRequest apiRequest,
            ContainerRequestContext containerRequestContext
    ) {
        ResponseBuilder rspBuilder = createResponseBuilder(
                preResponse.getResultSet(),
                preResponse.getResponseContext(),
                apiRequest,
                containerRequestContext
        );

        @SuppressWarnings("unchecked")
        MultivaluedMap<String, Object> headers = (MultivaluedMap<String, Object>) preResponse
                .getResponseContext()
                .get(HEADERS.getName());

        //Headers are a multivalued map, and we want to add each element of each value to the builder.
        headers.entrySet().stream()
                .forEach(entry -> entry.getValue().forEach(value -> rspBuilder.header(entry.getKey(), value)));

        return rspBuilder.build();
    }

    /**
     * Create a response builder with all the associated meta data.
     *
     * @param resultSet  The result set being processed
     * @param responseContext  A meta data container for the state gathered by the web container
     * @param apiRequest  ApiRequest object which contains request related information
     * @param containerRequestContext The container for jersey request processing objects
     *
     * @return Build response with requested format and associated meta data info.
     */
    protected ResponseBuilder createResponseBuilder(
            ResultSet resultSet,
            ResponseContext responseContext,
            ApiRequest apiRequest,
            ContainerRequestContext containerRequestContext
    ) {
        ResponseFormatType responseFormatType = apiRequest.getFormat();
        @SuppressWarnings("unchecked")
        Map<String, URI> bodyLinks = (Map<String, URI>) responseContext.get(
                PAGINATION_LINKS_CONTEXT_KEY.getName()
        );
        if (bodyLinks == null) {
            bodyLinks = Collections.emptyMap();
        }
        Pagination<?> pagination = (Pagination<?>) responseContext.get(PAGINATION_CONTEXT_KEY.getName());

        // Add headers for content type
        // default response format is JSON
        if (responseFormatType == null) {
            responseFormatType = DefaultResponseFormatType.JSON;
        }

        @SuppressWarnings("unchecked")
        LinkedHashMap<String, LinkedHashSet<DimensionField>> dimensionToDimensionFieldMap =
                (LinkedHashMap<String, LinkedHashSet<DimensionField>>) responseContext.get(
                        REQUESTED_API_DIMENSION_FIELDS.getName());


        LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> requestedApiDimensionFields =
                dimensionToDimensionFieldMap
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                e -> dimensionDictionary.findByApiName(e.getKey()),
                                Map.Entry::getValue,
                                (value1, value2) -> value1,
                                // We won't have any collisions, so just take the first value
                                LinkedHashMap::new
                        ));

        @SuppressWarnings("unchecked")
        ResponseData responseData = buildResponseData(
                resultSet,
                (LinkedHashSet<String>) responseContext.get(API_METRIC_COLUMN_NAMES.getName()),
                requestedApiDimensionFields,
                getPartialIntervalsWithDefault(responseContext),
                getVolatileIntervalsWithDefault(responseContext),
                pagination,
                bodyLinks
        );

        StreamingOutput stream = outputStream -> {
            responseWriter.write(apiRequest, responseData, outputStream);
        };

//      pass stream handler as response
        ResponseBuilder rspBuilder = javax.ws.rs.core.Response.ok(stream);
        return buildAndAddResponseHeaders(
                rspBuilder,
                responseFormatType,
                containerRequestContext,
                apiRequest.getDownloadFilename().orElse(null)
        );
    }

    /**
     * Prepare Response object from error details with reason and description.
     *
     * @param statusCode  Error status code
     * @param reason  Brief reason about the error
     * @param description  Description of the error
     * @param druidQuery  Druid query associated with the an error
     *
     * @return Publishable Response object
     */
    public javax.ws.rs.core.Response buildErrorResponse(
            int statusCode,
            String reason,
            String description,
            DruidQuery<?> druidQuery
    ) {
        return RequestHandlerUtils.makeErrorResponse(
                statusCode,
                reason,
                description,
                druidQuery,
                objectMappers.getMapper().writer()
        );
    }

    /**
     * Builds a ResponseData object.
     *
     * @param resultSet  ResultSet to turn into response
     * @param apiMetricColumnNames  The names of the logical metrics requested
     * @param requestedApiDimensionFields  The fields for each dimension that should be shown in the response
     * @param partialIntervals  intervals over which partial data exists
     * @param volatileIntervals  intervals over which data is understood as 'best-to-date'
     * @param pagination  The object containing the pagination information. Null if we are not paginating
     * @param paginationLinks A mapping from link names to links to be added to the end of the JSON response
     *
     * @return a new ResponseData object
     */
    protected ResponseData buildResponseData(
            ResultSet resultSet,
            LinkedHashSet<String> apiMetricColumnNames,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> requestedApiDimensionFields,
            SimplifiedIntervalList partialIntervals,
            SimplifiedIntervalList volatileIntervals,
            Pagination<?> pagination,
            Map<String, URI> paginationLinks
    ) {
        return new ResponseData(
                resultSet,
                apiMetricColumnNames,
                requestedApiDimensionFields,
                partialIntervals,
                volatileIntervals,
                pagination,
                paginationLinks
        );
    }

    /**
     * Builds the headers for the response and gives them to the provided response builder to be added to the response.
     *
     * @param rspBuilder  ResponseBuilder that handles adding the headers to the response
     * @param responseFormatType  The type of the response
     * @param containerRequestContext  The request context
     *
     * @return the response builder that has had the headers added
     */
    protected ResponseBuilder buildAndAddResponseHeaders(
            ResponseBuilder rspBuilder,
            ResponseFormatType responseFormatType,
            ContainerRequestContext containerRequestContext
    ) {
        return buildAndAddResponseHeaders(rspBuilder, responseFormatType, containerRequestContext, null);
    }

    /**
     * Builds the headers for the response and gives them to the provided response builder to be added to the response.
     *
     * @param rspBuilder  ResponseBuilder that handles adding the headers to the response
     * @param responseFormatType  The type of the response
     * @param containerRequestContext  The request context
     * @param downloadFilename  The filename the response should be downloaded as. Null or empty indicates the response
     * should not be downloaded and instead rendered by the browser
     *
     * @return the response builder that has had the headers added
     */
    protected ResponseBuilder buildAndAddResponseHeaders(
            ResponseBuilder rspBuilder,
            ResponseFormatType responseFormatType,
            ContainerRequestContext containerRequestContext,
            String downloadFilename
    ) {
        Map<String, String> responseHeaders = responseUtils.buildResponseFormatHeaders(
                containerRequestContext,
                downloadFilename,
                responseFormatType
        );

        ResponseBuilder responseBuilderWithHeaders = rspBuilder;
        for (Map.Entry<String, String> entry : responseHeaders.entrySet()) {
            responseBuilderWithHeaders = responseBuilderWithHeaders.header(entry.getKey(), entry.getValue());
        }
        return responseBuilderWithHeaders;
    }
}
