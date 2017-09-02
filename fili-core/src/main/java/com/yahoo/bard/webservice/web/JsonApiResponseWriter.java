// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionField;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;


/**
 * Serializer for JsonApi format.
 *
 * The response when serialized is in the following format:
 * <pre>
 * {@code{
 *   "rows" : [
 *     {
 *       "dateTime" : "YYYY-MM-dd HH:mm:ss.SSS",
 *       "logicalMetric1Name" : logicalMetric1Value,
 *       "logicalMetric2Name" : logicalMetric2Value,
 *       ...
 *       "dimension1Name" : "dimension1ValueKeyValue1",
 *       "dimension2Name" : "dimension2ValueKeyValue1",
 *       ...
 *     }, {
 *      ...
 *     }
 *   ],
 *   "dimension1Name" : [
 *     {
 *       "dimension1KeyFieldName" : "dimension1KeyFieldValue1",
 *       "dimension1OtherFieldName" : "dimension1OtherFieldValue1"
 *     }, {
 *       "dimension1KeyFieldName" : "dimension1KeyFieldValue2",
 *       "dimension1OtherFieldName" : "dimension1OtherFieldValue2"
 *     },
 *     ...
 *   ],
 *   "dimension2Name" : [
 *     {
 *       "dimension2KeyFieldName" : "dimension2KeyFieldValue1",
 *       "dimension2OtherFieldName" : "dimension2OtherFieldValue1"
 *     }, {
 *       "dimension2KeyFieldName" : "dimension2KeyFieldValue2",
 *       "dimension2OtherFieldName" : "dimension2OtherFieldValue2"
 *     },
 *     ...
 *   ]
 *   "linkName1" : "http://uri1",
 *   "linkName2": "http://uri2",
 *   ...
 *   "linkNameN": "http://uriN"
 * }
 * }
 * </pre>
 *
 * Where "linkName1" ... "linkNameN" are the N keys in paginationLinks, and "http://uri1" ... "http://uriN" are the
 * associated URI's.
 */
public class JsonApiResponseWriter extends JsonAndJsonApiResponseWriter {

    private static final Logger LOG = LoggerFactory.getLogger(JsonApiResponseWriter.class);

    /**
     * Constructor.
     *
     * @param objectMappers  ObjectMappersSuite object needed for JsonFactory
     *
     */
    public JsonApiResponseWriter(ObjectMappersSuite objectMappers) {
        super(objectMappers);
    }

    /**
     * Writes JSON-API response.
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
        try (JsonGenerator generator = jsonFactory.createGenerator(os)) {
            // Holder for the dimension rows in the result set
            Map<Dimension, Set<Map<DimensionField, String>>> sidecars = new HashMap<>();
            for (DimensionColumn dimensionColumn :
                    responseData.getResultSet().getSchema().getColumns(DimensionColumn.class)) {
                sidecars.put(dimensionColumn.getDimension(), new LinkedHashSet<>());
            }

            // Start the top-level JSON object
            generator.writeStartObject();

            // Write the data rows and extract the dimension rows for the sidecars
            generator.writeArrayFieldStart("rows");
            for (Result result : responseData.getResultSet()) {
                generator.writeObject(responseData.buildResultRowWithSidecars(result, sidecars));
            }
            generator.writeEndArray();

            // Write the sidecar for each dimension
            for (Map.Entry<Dimension, Set<Map<DimensionField, String>>> sidecar : sidecars.entrySet()) {
                generator.writeArrayFieldStart(sidecar.getKey().getApiName());
                for (Map<DimensionField, String> dimensionRow : sidecar.getValue()) {

                    // Write each of the sidecar rows
                    generator.writeStartObject();
                    for (DimensionField dimensionField : dimensionRow.keySet()) {
                        generator.writeObjectField(dimensionField.getName(), dimensionRow.get(dimensionField));
                    }
                    generator.writeEndObject();
                }
                generator.writeEndArray();
            }

            super.writeMetaObject(
                    generator,
                    responseData.getMissingIntervals(),
                    responseData.getVolatileIntervals(),
                    responseData
            );

            // End the top-level JSON object
            generator.writeEndObject();
        } catch (IOException e) {
            LOG.error("Unable to write JSON: {}", e.toString());
            throw e;
        }
    }
}
