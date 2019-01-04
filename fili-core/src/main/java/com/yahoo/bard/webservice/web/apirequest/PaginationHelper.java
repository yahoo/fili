// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.util.AllPagesPagination;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.responseprocessors.MappingResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys;
import com.yahoo.bard.webservice.web.util.PaginationLink;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

/**
 * A utility class to separate out the pagination capability from.
 */
public class PaginationHelper {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final int DEFAULT_PER_PAGE = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("default_per_page")
    );

    private static final int DEFAULT_PAGE = 1;
    private static final PaginationParameters DEFAULT_PAGINATION = new PaginationParameters(
            DEFAULT_PER_PAGE,
            DEFAULT_PAGE
    );

    private static final String PAGE = "page";
    private static final String PER_PAGE = "perPage";

    protected Pagination pagination;
    protected final PaginationParameters paginationParameters;
    protected final UriInfo uriInfo;
    protected final Response.ResponseBuilder builder;

    /**
     * Constructor.
     * Uses the default Pagination.
     *
     * @param uriInfo  The uri info used to build pagination links
     */
    public PaginationHelper(UriInfo uriInfo) {
        this(uriInfo, getDefaultPagination(), Response.status(Response.Status.OK));
    }

    /**
     * Constructor.
     *
     * @param uriInfo  The uri info used to build pagination links
     * @param paginationParameters  parameters describing the pagination
     */
    public PaginationHelper(UriInfo uriInfo, PaginationParameters paginationParameters) {
        this(uriInfo, paginationParameters, Response.status(Response.Status.OK));
    }

    /**
     * Constructor.
     *
     * @param uriInfo  The uri info used to build pagination links
     * @param page  the page number of the desired result
     * @param perPage  the number of records per page.
     */
    public PaginationHelper(UriInfo uriInfo, String page, String perPage) {
        this(
                uriInfo,
                DefaultOutputFormatGenerators.INSTANCE.generatePaginationParameters(perPage, page).orElse(null),
                Response.status(Response.Status.OK)
        );
    }

    /**
     * Constructor.
     *
     * @param uriInfo  The uri info used to build pagination links
     * @param perPage  the number of records per page.
     * @param page  the page number of the desired result
     * @param builder  A builder for a response (if not using a default one)
     */
    public PaginationHelper(UriInfo uriInfo, String perPage, String page, Response.ResponseBuilder builder) {
        this(
                uriInfo,
                DefaultOutputFormatGenerators.INSTANCE.generatePaginationParameters(perPage, page).orElse(null),
                builder
        );
    }

    /**
     * Constructor.
     *
     * @param uriInfo  The uri info used to build pagination links
     * @param paginationParameters  parameters describing the pagination
     * @param builder  A builder for a response (if not using a default one)
     */
    public PaginationHelper(
            UriInfo uriInfo,
            PaginationParameters paginationParameters,
            Response.ResponseBuilder builder
    ) {
        this.uriInfo = uriInfo;
        this.paginationParameters = paginationParameters != null ? paginationParameters : getDefaultPagination();
        this.builder = builder;
    }

    /**
     * Get the default pagination parameters for this type of API request.
     *
     * @return The uri info of this type of API request
     */
    public static PaginationParameters getDefaultPagination() {
        return DEFAULT_PAGINATION;
    }

    /**
     * This method returns a Function that can basically take a Collection and return an instance of
     * AllPagesPagination.
     *
     * @param paginationParameters  The PaginationParameters to be used to generate AllPagesPagination instance
     * @param <T>  The type of items in the Collection which needs to be paginated
     *
     * @return A Function that takes a Collection and returns an instance of AllPagesPagination
     */
    public static <T> Function<Collection<T>, AllPagesPagination<T>> getAllPagesPaginationFactory(
            PaginationParameters paginationParameters
    ) {
        return data -> new AllPagesPagination<>(data, paginationParameters);
    }


    /**
     * Getter.
     *
     * @return  The pagination parameters fueling this page helper.
     */
    public PaginationParameters getPaginationParameters() {
        return paginationParameters;
    }

    /**
     * Add page links to the header of the response builder.
     *
     * @param link  The type of the link to add.
     * @param pages  The paginated set of results containing the pages being linked to.
     */
    protected void addPageLink(PaginationLink link, Pagination<?> pages) {
        link.getPage(pages).ifPresent(page -> addPageLink(link, page));
    }

    /**
     * Add page links to the header of the response builder.
     *
     * @param link  The type of the link to add.
     * @param pageNumber  Number of the page to add the link for.
     */
    protected void addPageLink(PaginationLink link, int pageNumber) {
        UriBuilder uriBuilder = uriInfo.getRequestUriBuilder().replaceQueryParam("page", pageNumber);
        builder.header(HttpHeaders.LINK, Link.fromUriBuilder(uriBuilder).rel(link.getHeaderName()).build());
    }


    /**
     * Add links to the response builder and return a stream with the requested page from the raw data.
     *
     * @param <T>  The type of the collection elements
     * @param data  The data to be paginated.
     *
     * @return A stream corresponding to the requested page.
     *
     * @deprecated Pagination is moving to a Stream and pushing creation of the page to a more general
     * method ({@link #getPage(Pagination)}) to allow for more flexibility
     * in how pagination is done.
     */
    @Deprecated
    public <T> Stream<T> getPage(Collection<T> data) {
        return getPage(new AllPagesPagination<>(data, paginationParameters));
    }

    /**
     * Add links to the response builder and return a stream with the requested page from the raw data.
     *
     * @param <T>  The type of the collection elements
     * @param pagination  The pagination object
     *
     * @return A stream corresponding to the requested page.
     */
    public <T> Stream<T> getPage(Pagination<T> pagination) {
        this.pagination = pagination;

        Arrays.stream(PaginationLink.values()).forEachOrdered(link -> addPageLink(link, pagination));

        return pagination.getPageOfData().stream();
    }

    /**
     * Return the links to put in the message body of a paginated collection.
     *
     * @param pages The Pagination iterator
     * @param uriBuilder  The UriBuilder used to build the links
     *
     * @return  A map of page links
     */
    public static Map<String, URI> getBodyLinks(Pagination<?> pages, UriBuilder uriBuilder) {
        Map<String, URI> bodyLinks = new LinkedHashMap<>();

        Arrays.stream(PaginationLink.values()).forEachOrdered(link -> addLink(link, pages, uriBuilder, bodyLinks));
        return bodyLinks;
    }

    /**
     * Returns all required pagination links.
     *
     * @param pages  The paginated set of results containing the pages being linked to.
     * @param uriBuilder  The builder for creating the pagination links.
     *
     * @return the map of metadata for this response
     */
    public static Map<String, Object> addLinks(Pagination<?> pages, UriBuilder uriBuilder) {
        Map<String, URI> bodyLinks = getBodyLinks(pages, uriBuilder);

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
}
