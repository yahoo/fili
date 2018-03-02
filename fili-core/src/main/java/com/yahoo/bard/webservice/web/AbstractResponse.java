// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.responseprocessors.MappingResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys;
import com.yahoo.bard.webservice.web.util.PaginationLink;

import com.fasterxml.jackson.core.JsonGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Link;
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
    private static final String PAGE = "page";
    private static final String PER_PAGE = "perPage";

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
     * Returns all required pagination links.
     *
     * @param pages  The paginated set of results containing the pages being linked to.
     * @param uriBuilder  The builder for creating the pagination links.
     *
     * @return the map of metadata for this response
     */
    public static Map<String, Object> addLinks(Pagination<?> pages, UriBuilder uriBuilder) {
        Map<String, URI> bodyLinks = new LinkedHashMap<>();

        Arrays.stream(PaginationLink.values()).forEachOrdered(link -> addLink(link, pages, uriBuilder, bodyLinks));

        Map<String, Object> metaBlock = new LinkedHashMap<>();
        if (!bodyLinks.isEmpty()) {
            metaBlock.put(ResponseContextKeys.PAGINATION_LINKS_CONTEXT_KEY.getName(), bodyLinks);
        }

        return metaBlock;
    }

    /**
     * Adds the specified link to the headers and body of the response.
     *
     * @param link  The type of the link being added
     * @param pages  The paginated set of results containing the pages being linked to.
     * @param uriBuilder  The builder for creating the pagination links.
     * @param bodyLinks  The map of links that will be added to the body of the response
     */
    public static void addLink(
            PaginationLink link,
            Pagination<?> pages,
            UriBuilder uriBuilder,
            Map<String, URI> bodyLinks
    ) {
        link.getPage(pages).ifPresent(page -> addLink(link, page, pages.getPerPage(), uriBuilder, bodyLinks));
    }

    /**
     * Adds the specified link to the map of links that will be added to the body of the response.
     *
     * @param paginationLink  The type of the link being added
     * @param pageNumber  The page being linked to
     * @param perPage  The number of result rows in the page being linked to
     * @param uriBuilder  The builder for creating the pagination links
     * @param bodyLinks  The map of links that will be added to the body of the response
     */
    private static void addLink(
            PaginationLink paginationLink,
            int pageNumber,
            int perPage,
            UriBuilder uriBuilder,
            Map<String, URI> bodyLinks
    ) {
        bodyLinks.put(
                paginationLink.getBodyName(),
                uriBuilder
                        .replaceQueryParam(PAGE, pageNumber)
                        .replaceQueryParam(PER_PAGE, perPage)
                        .build()
        );
    }

    /**
     * Adds all the required pagination links to the headers and body of the response.
     *
     * @param pages  The paginated set of results containing the pages being linked to.
     * @param uriBuilder  The uri builder to build the links
     * @param responseProcessor  The response processor whose links are being built
     */
    public static void addLinks(
            Pagination<?> pages,
            UriBuilder uriBuilder,
            MappingResponseProcessor responseProcessor
    ) {
        LinkedHashMap<String, URI> bodyLinks = new LinkedHashMap<>();

        Arrays.stream(PaginationLink.values())
                .forEachOrdered(link -> addLink(link, pages, uriBuilder, responseProcessor, bodyLinks));

        ResponseContext responseContext = responseProcessor.getResponseContext();
        responseContext.put(ResponseContextKeys.PAGINATION_LINKS_CONTEXT_KEY.getName(), bodyLinks);
        responseContext.put(ResponseContextKeys.PAGINATION_CONTEXT_KEY.getName(), pages);
    }

    /**
     * Adds the specified link to the headers and body of the response.
     *
     * @param link  The type of the link being added
     * @param pages  The paginated set of results containing the pages being linked to.
     * @param uriBuilder  The uri builder to build the links
     * @param responseProcessor  The response processor whose links are being built
     * @param bodyLinks  The map of links that will be added to the body of the response
     */
    public static void addLink(
            PaginationLink link,
            Pagination<?> pages,
            UriBuilder uriBuilder,
            MappingResponseProcessor responseProcessor,
            Map<String, URI> bodyLinks
    ) {
        link.getPage(pages).ifPresent(
                page -> addLink(link, page, pages.getPerPage(), uriBuilder, responseProcessor, bodyLinks)
        );
    }

    /**
     * Adds the specified link to the headers and to the map of links that will be added to the body of the response.
     *
     * @param link  The type of the link being added
     * @param pageNumber  The page being linked to
     * @param perPage  The number of result rows in the page being linked to
     * @param uriBuilder  The uri builder to build the links
     * @param responseProcessor  The response processor whose links are being built
     * @param bodyLinks  The map of links that will be added to the body of the response
     */
    public static void addLink(
            PaginationLink link,
            int pageNumber,
            int perPage,
            UriBuilder uriBuilder,
            MappingResponseProcessor responseProcessor,
            Map<String, URI> bodyLinks
    ) {
        UriBuilder pageLink = uriBuilder.replaceQueryParam(PAGE, pageNumber).replaceQueryParam(PER_PAGE, perPage);
        responseProcessor.getHeaders().add(
                HttpHeaders.LINK,
                Link.fromUriBuilder(pageLink).rel(link.getHeaderName()).build().toString()
        );
        bodyLinks.put(link.getBodyName(), pageLink.build());
    }

    /**
     * Builds the meta data entry of the page if such data is present.
     * Currently it contains only the pagination related metadata.
     *
     * @param generator  The JsonGenerator used to build the JSON response.
     * @param uriBuilder  The builder for creating the pagination links.
     *
     * @throws IOException if the generator throws an IOException.
     */
    public void writeMetaObject(JsonGenerator generator, UriBuilder uriBuilder) throws IOException {
        Map<String, Object> meta = pages == null ? Collections.emptyMap() : addLinks(pages, uriBuilder);

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
