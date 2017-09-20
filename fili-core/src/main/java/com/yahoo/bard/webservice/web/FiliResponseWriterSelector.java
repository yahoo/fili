// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;


import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * FiliResponseWriterSelector is the default selector for ReponseWriterSelector. It selects the writer based on the
 * format type in the ApiRequest. It's initialized with three format-writer mapping: CSV, Json and JsonApi.
 */
public class FiliResponseWriterSelector implements ResponseWriterSelector {
    private final Map<ResponseFormatType, ResponseWriter> writers;

    /**
     * Constructor for default writer selector. Initialize format to writer mapping.
     *
     * @param csvResponseWriter The CSV writer which serialize output into csv format
     * @param jsonResponseWriter  The Json writer which serialize output into csv format
     * @param jsonApiResponseWriter  The JsonApi writer which serialize output into csv format
     */
    public FiliResponseWriterSelector(
            CsvResponseWriter csvResponseWriter,
            JsonResponseWriter jsonResponseWriter,
            JsonApiResponseWriter jsonApiResponseWriter
    ) {
        writers = new HashMap<>();
        writers.put(ResponseFormatType.CSV, csvResponseWriter);
        writers.put(ResponseFormatType.JSON, jsonResponseWriter);
        writers.put(ResponseFormatType.JSONAPI, jsonApiResponseWriter);
    }

    /**
     * Selects a ResponseWriter given the format type from request.
     *
     * @param request  ApiRequest object with all the associated info in it
     * @return Response writer for the given format type
     */
    @Override
    public Optional<ResponseWriter> select(ApiRequest request) {
        ResponseFormatType format = request.getFormat();
        if (format == null) {
            format = ResponseFormatType.JSON;
        }
        return Optional.ofNullable(writers.get(format));
    }

    /**
     * Add a customized format writer pair to the Map.
     *
     * @param type  Custom format type.
     * @param writer Writer which should be used to do serialization with the type.
     */
    public void addWriter(ResponseFormatType type, ResponseWriter writer) {
        writers.put(type, writer);
    }
}
