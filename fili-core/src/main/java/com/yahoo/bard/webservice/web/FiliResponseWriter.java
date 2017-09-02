// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;


/**
 * Default writer for response. FiliResponseWriter choose a proper writer type using FiliResponseWriterSelector.
 */
public class FiliResponseWriter implements ResponseWriter {

    private final ResponseWriterSelector responseWriterSelector;
    private static final Logger LOG = LoggerFactory.getLogger(FiliResponseWriter.class);

    /**
     * Constructor.
     *
     * @param responseWriterSelector A strategy for deciding which ResponseWriter to use to serialize the ResultSet.
     */
    public FiliResponseWriter(ResponseWriterSelector responseWriterSelector) {
        this.responseWriterSelector = responseWriterSelector;
    }

    @Override
    public void write(
            ApiRequest request,
            ResponseData responseData,
            OutputStream os
    ) throws IOException {
        Optional<ResponseWriter> writer = responseWriterSelector.select(request);
        if (!writer.isPresent()) {
            String errorMsg = "Format type " + request.getFormat() + " is not recognized.";
            LOG.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }
        writer.get().write(request, responseData, os);
    }
}
