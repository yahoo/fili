// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.apirequest.PaginationHelper;

import com.fasterxml.jackson.core.JsonGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

/**
 * Formats data of a response as JSON.
 *
 * @param <T> the type of the raw data
 */
public abstract class AbstractResponse<T> implements ResponseStream {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractResponse.class);

    protected final Stream<T> entries;
    protected final UriInfo uriInfo;
    protected final Pagination<?> pages;
    protected final ObjectMappersSuite objectMappers;

    /**
     * Constructor.
     *
     * @param entries  The data entries to generate the response for
     * @param uriInfo  UriInfo to generate the URL for the page links
     * @param pages  The paginated set of results containing the pages being linked to.
     * @param objectMappers  Suite of Object Mappers to use when serializing the response
     */
    public AbstractResponse(Stream<T> entries, Pagination<?> pages, UriInfo uriInfo, ObjectMappersSuite objectMappers) {
        this.entries = entries;
        this.pages = pages;
        this.uriInfo = uriInfo;
        this.objectMappers = objectMappers;
    }

    /**
     * Get a resource method that can be used to stream this response as an entity.
     *
     * @return The resource method
     */
    @Override
    public StreamingOutput getResponseStream() {
        return this::write;
    }

    /**
     * Write the response to the output stream.
     *
     * @param outputStream  Output stream to write to
     * @throws IOException if there's a problem writing to the stream
     */
    public abstract void write(OutputStream outputStream) throws IOException;

    /**
     * Builds the meta data entry of the page if such data is present.
     * Currently it contains only the pagination related metadata.
     *
     * @param generator  The JsonGenerator used to build the JSON response.
     * @param uriBuilder  The builder for creating the pagination links.
     *
     * @throws IOException if the generator throws an IOException.
     */
    public void writeMetaObject(JsonGenerator generator, UriBuilder uriBuilder)
            throws IOException {
        Map<String, Object> meta = pages == null ? Collections.emptyMap() : PaginationHelper.addLinks(pages, uriBuilder);

        if (meta.isEmpty()) {
            return;
        }

        generator.writeObjectFieldStart("meta");
        generator.writeObjectFieldStart("pagination");

        try {
            meta.forEach(
                    (k, v) -> {
                        try {
                            generator.writeObjectField(k, v);
                        } catch (IOException ioe) {
                            String msg = String.format("Unable to write pagination field with key %s", k);
                            LOG.error(msg, ioe);
                            throw new RuntimeException(msg, ioe);
                        }
                    }
            );
        } catch (RuntimeException re) {
            throw new IOException(re);
        }

        generator.writeNumberField("currentPage", pages.getPage());
        generator.writeNumberField("rowsPerPage", pages.getPerPage());
        generator.writeNumberField("numberOfResults", pages.getNumResults());
        generator.writeEndObject();
        generator.writeEndObject();
    }
}
