// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.data;

import static com.yahoo.fili.webservice.web.handlers.PartialDataRequestHandler.getPartialIntervalsWithDefault;
import static com.yahoo.fili.webservice.web.handlers.VolatileDataRequestHandler.getVolatileIntervalsWithDefault;
import static com.yahoo.fili.webservice.web.responseprocessors.ResponseContextKeys.API_METRIC_COLUMN_NAMES;
import static com.yahoo.fili.webservice.web.responseprocessors.ResponseContextKeys.HEADERS;
import static com.yahoo.fili.webservice.web.responseprocessors.ResponseContextKeys.PAGINATION_CONTEXT_KEY;
import static com.yahoo.fili.webservice.web.responseprocessors.ResponseContextKeys.PAGINATION_LINKS_CONTEXT_KEY;
import static com.yahoo.fili.webservice.web.responseprocessors.ResponseContextKeys.REQUESTED_API_DIMENSION_FIELDS;

import com.yahoo.fili.webservice.application.ObjectMappersSuite;
import com.yahoo.fili.webservice.data.dimension.Dimension;
import com.yahoo.fili.webservice.data.dimension.DimensionDictionary;
import com.yahoo.fili.webservice.data.dimension.DimensionField;
import com.yahoo.fili.webservice.druid.model.query.DruidQuery;
import com.yahoo.fili.webservice.util.Pagination;
import com.yahoo.fili.webservice.web.PreResponse;
import com.yahoo.fili.webservice.web.Response;
import com.yahoo.fili.webservice.web.ResponseFormatType;
import com.yahoo.fili.webservice.web.handlers.RequestHandlerUtils;
import com.yahoo.fili.webservice.web.responseprocessors.ResponseContext;
import com.yahoo.fili.webservice.web.util.ResponseFormat;

import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Singleton;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;

/**
 * Translates a PreResponse into an HTTP Response containing the results of a query.
 */
@Singleton
public class HttpResponseMaker {

    private final ObjectMappersSuite objectMappers;
    private final DimensionDictionary dimensionDictionary;

    /**
     * Class constructor.
     *
     * @param objectMappers  Mappers object for serialization
     * @param dimensionDictionary  The dimension dictionary from which to look up dimensions by name
     */
    public HttpResponseMaker(ObjectMappersSuite objectMappers, DimensionDictionary dimensionDictionary) {
        this.objectMappers = objectMappers;
        this.dimensionDictionary = dimensionDictionary;
    }

    /**
     * Build complete response.
     *
     * @param preResponse  PreResponse object which contains result set, response context and headers
     * @param responseFormatType  The format in which the response should be returned to the user
     * @param uriInfo  UriInfo of the request
     *
     * @return Completely built response with headers and result set
     */
    public javax.ws.rs.core.Response buildResponse(
            PreResponse preResponse,
            ResponseFormatType responseFormatType,
            UriInfo uriInfo
    ) {
        ResponseBuilder rspBuilder = createResponseBuilder(
                preResponse.getResultSet(),
                preResponse.getResponseContext(),
                responseFormatType,
                uriInfo
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
     * @param responseFormatType  The format in which the response should be returned to the user
     * @param uriInfo  UriInfo of the request
     *
     * @return Build response with requested format and associated meta data info.
     */
    private ResponseBuilder createResponseBuilder(
            ResultSet resultSet,
            ResponseContext responseContext,
            ResponseFormatType responseFormatType,
            UriInfo uriInfo
    ) {
        @SuppressWarnings("unchecked")
        Map<String, URI> bodyLinks = (Map<String, URI>) responseContext.get(
                PAGINATION_LINKS_CONTEXT_KEY.getName()
        );
        if (bodyLinks == null) {
            bodyLinks = Collections.emptyMap();
        }
        Pagination pagination = (Pagination) responseContext.get(PAGINATION_CONTEXT_KEY.getName());

        // Add headers for content type
        // default response format is JSON
        if (responseFormatType == null) {
            responseFormatType = ResponseFormatType.JSON;
        }

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

        Response response = new Response(
                resultSet,
                (LinkedHashSet<String>) responseContext.get(API_METRIC_COLUMN_NAMES.getName()),
                requestedApiDimensionFields,
                responseFormatType,
                getPartialIntervalsWithDefault(responseContext),
                getVolatileIntervalsWithDefault(responseContext),
                bodyLinks,
                pagination,
                objectMappers
        );

        // pass stream handler as response
        ResponseBuilder rspBuilder = javax.ws.rs.core.Response.ok(
                response.getResponseStream()
        );

        // build response
        switch (responseFormatType) {
            case CSV:
                return rspBuilder
                        .header(HttpHeaders.CONTENT_TYPE, "text/csv; charset=utf-8")
                        .header(
                                HttpHeaders.CONTENT_DISPOSITION,
                                ResponseFormat.getCsvContentDispositionValue(uriInfo)
                        );
            case JSON:
                // Fall-through: Default is JSON
            default:
                return rspBuilder
                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON + "; charset=utf-8");
        }
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
}
