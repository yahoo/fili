// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.Result;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Serializer for Json format.
 *
 * The response when serialized is in the following format
 * <pre>
 * {@code
 * {
 *     "metricColumn1Name" : metricValue1,
 *     "metricColumn2Name" : metricValue2,
 *     .
 *     .
 *     .
 *     "dimensionColumnName" : "dimensionRowDesc",
 *     "dateTime" : "formattedDateTimeString"
 * },
 *   "linkName1" : "http://uri1",
 *   "linkName2": "http://uri2",
 *   ...
 *   "linkNameN": "http://uriN"
 * }
 * </pre>
 *
 * Where "linkName1" ... "linkNameN" are the N keys in paginationLinks, and "http://uri1" ... "http://uriN" are the
 * associated URI's.
 */
public class JsonResponseWriter extends JsonAndJsonApiResponseWriter {

    private static final Logger LOG = LoggerFactory.getLogger(JsonResponseWriter.class);

    /**
     * Constructor.
     *
     * @param objectMappers  ObjectMappersSuite object needed for JsonFactory
     *
     */
    public JsonResponseWriter(
            ObjectMappersSuite objectMappers
    ) {
        super(objectMappers);
    }

    /**
     * Writes JSON response.
     *
     * @param request  ApiRequest object with all the associated info in it
     * @param responseData  Data object containing all the result information
     * @param os  OutputStream
     *
     * @throws IOException if a problem is encountered writing to the OutputStream
     */
    @Override
    public void write(
            ApiRequest request,
            ResponseData responseData,
            OutputStream os
    ) throws IOException {
        JsonFactory jsonFactory = new JsonFactory(getObjectMappers().getMapper());
        try (JsonGenerator g = jsonFactory.createGenerator(os)) {
            g.writeStartObject();

            g.writeArrayFieldStart("rows");
            for (Result result : responseData.getResultSet()) {
                g.writeObject(responseData.buildResultRow(result));
            }
            g.writeEndArray();

            super.writeMetaObject(g, responseData.getMissingIntervals(),
                    responseData.getVolatileIntervals(), responseData
            );

            g.writeEndObject();
        } catch (IOException e) {
            LOG.error("Unable to write JSON: {}", e.toString());
            throw e;
        }
    }
}
