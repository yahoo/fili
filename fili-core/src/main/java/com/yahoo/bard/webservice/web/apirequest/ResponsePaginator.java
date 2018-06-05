// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.util.PaginationLink;

import java.util.Arrays;
import java.util.stream.Stream;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;


/**
 * Static methods to assist in creating links to paginated views on collections of data or metadata.
 */
public class ResponsePaginator {

    /**
     * Add page links to the header of the response builder.
     *
     * @param responseBuilder  The builder for user responses
     * @param link  The type of the link to add.
     * @param uriInfo The uri info for building page links
     * @param pages  The paginated set of results containing the pages being linked to.
     */
    protected static void addPageLink(
            Response.ResponseBuilder responseBuilder,
            PaginationLink link,
            UriInfo uriInfo,
            Pagination<?> pages
    ) {
        link.getPage(pages).ifPresent(page -> addPageLink(responseBuilder, link, uriInfo, page));
    }

    /**
     * Add page links to the header of the response builder.
     *
     * @param link  The type of the link to add.
     * @param uriInfo The uri info for building page links
     * @param pageNumber  Number of the page to add the link for.
     * @param responseBuilder The builder for the http response
     */
    protected static void addPageLink(
            Response.ResponseBuilder responseBuilder,
            PaginationLink link,
            UriInfo uriInfo,
            int pageNumber
    ) {
        UriBuilder uriBuilder = uriInfo.getRequestUriBuilder().replaceQueryParam("page", pageNumber);
        responseBuilder.header(HttpHeaders.LINK, Link.fromUriBuilder(uriBuilder).rel(link.getHeaderName()).build());
    }

    /**
     * Add links to the response builder and return a stream with the requested page from the raw data.
     *
     * @param <T>  The type of the collection elements
     * @param pagination  The pagination object
     * @param responseBuilder The builder for the http response
     * @param uriInfo  The context object desribing the requested uri
     *
     * @return  A stream from the subcollection of the data collection corresponding to the page described
     */

    public static <T> Stream<T> paginate(
            Response.ResponseBuilder responseBuilder,
            Pagination<T> pagination,
            UriInfo uriInfo
    ) {
        Arrays.stream(PaginationLink.values())
                .forEachOrdered(link -> addPageLink(responseBuilder, link, uriInfo, pagination));

        return pagination.getPageOfData().stream();
    }
}
