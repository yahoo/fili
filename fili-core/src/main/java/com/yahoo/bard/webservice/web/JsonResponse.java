// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.util.Pagination;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.stream.Stream;

import javax.ws.rs.core.UriInfo;

/**
 * Formats data of a response as JSON.
 *
 * @param <T> the type of the raw data
 */
public class JsonResponse<T> extends AbstractResponse<T> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonResponse.class);

    private final String responseName;
    private final JsonFactory jsonFactory;

    /**
     * Constructor.
     *
     * @param entries  The data entries to generate the response for
     * @param pages  The paginated set of results containing the pages being linked to.
     * @param uriInfo  UriInfo to generate the URL for the page links
     * @param responseName  Top level name of json object described the response data.
     * @param objectMappers  Suite of Object Mappers to use when serializing the response
     */
    public JsonResponse(
            Stream<T> entries,
            Pagination<?> pages,
            UriInfo uriInfo,
            String responseName,
            ObjectMappersSuite objectMappers
    ) {
        super(entries, pages, uriInfo, objectMappers);
        this.responseName = responseName;
        this.jsonFactory = new JsonFactory(objectMappers.getMapper());
    }

    /**
     * Generates JSON response.
     * <p>
     * format: <pre>
     * {
     *     "responseName" : [
     *         {
     *             "id" : 1234,
     *             "description" : "description of dimension value",
     *         },
     *         {
     *             "id" : 1235,
     *             "description" : "another description",
     *         }
     *     ],
     *     "meta" : {
     *         "pagination" : {
     *             "paginationLinks" : {
     *                 "first" : "http://first_page_uri/", (optional)
     *                 "last" : "http://last_page_uri/", (optional)
     *                 "next" : "http://next_page_uri/", (optional)
     *                 "previous": "http://previous_page_uri/" (optional)
     *             },
     *             "currentPage": number of current page,
     *             "rowsPerPage": number of rows per page,
     *             "numberOfResults": number of results
     *         }
     *     }
     * }</pre>
     */
    @Override
    public void write(OutputStream os) throws IOException {
        try (JsonGenerator g = jsonFactory.createGenerator(os)) {
            g.writeStartObject();
            g.writeArrayFieldStart(responseName);
            try {
                entries.forEachOrdered(
                        entry -> {
                            try {
                                g.writeObject(entry);
                            } catch (IOException ioe) {
                                String msg = String.format("Unable to write Json Object: %s", entry);
                                LOG.error(msg, ioe);
                                throw new RuntimeException(msg, ioe);
                            }
                        }
                );
            } catch (RuntimeException re) {
                throw new IOException(re);
            }
            g.writeEndArray();

            writeMetaObject(g, uriInfo.getRequestUriBuilder());

            g.writeEndObject();
        } catch (IOException ioe) {
            LOG.error("Unable to write JSON: {}", ioe.toString());
            throw ioe;
        }
    }
}
