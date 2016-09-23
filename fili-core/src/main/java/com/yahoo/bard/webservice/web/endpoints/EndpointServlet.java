// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.web.ApiRequest;
import com.yahoo.bard.webservice.web.CsvResponse;
import com.yahoo.bard.webservice.web.JsonResponse;
import com.yahoo.bard.webservice.web.util.ResponseFormat;

import java.util.List;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

/**
 * Abstract class making available the common code between the servlets that serve the endpoints.
 */
public abstract class EndpointServlet {
    protected final ObjectMappersSuite objectMappers;

    /**
     * Constructor.
     *
     * @param objectMappers  Shared JSON tools
     */
    @Inject
    public EndpointServlet(ObjectMappersSuite objectMappers) {
        this.objectMappers = objectMappers;
    }

    /**
     * Format and build the response as JSON or CSV.
     *
     * @param apiRequest  The api request object
     * @param rows  The stream that describes the data to be formatted
     * @param jsonName  Top-level title for the JSON data
     * @param csvColumnNames  Header for the CSV data
     * @param <T> The type of rows being processed
     *
     * @return The updated response builder with the new link header added
     */
    protected <T> Response formatResponse(
            ApiRequest apiRequest,
            Stream<T> rows,
            String jsonName,
            List<String> csvColumnNames
    ) {
        StreamingOutput output;
        Response.ResponseBuilder builder;
        switch (apiRequest.getFormat()) {
            case CSV:
                builder = apiRequest.getBuilder()
                        .header(HttpHeaders.CONTENT_TYPE, "text/csv; charset=utf-8")
                        .header(
                                HttpHeaders.CONTENT_DISPOSITION,
                                ResponseFormat.getCsvContentDispositionValue(apiRequest.getUriInfo())
                        );
                output = new CsvResponse<>(
                        rows,
                        apiRequest.getPagination(),
                        apiRequest.getUriInfo(),
                        csvColumnNames,
                        objectMappers
                ).getResponseStream();
                break;
            case JSON:
                // Fall-through: Default is JSON
            default:
                builder = apiRequest.getBuilder()
                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON + "; charset=utf-8");

                output = new JsonResponse<>(
                        rows,
                        apiRequest.getPagination(),
                        apiRequest.getUriInfo(),
                        jsonName,
                        objectMappers
                ).getResponseStream();
                break;
        }
        return builder.entity(output).build();
    }
}
