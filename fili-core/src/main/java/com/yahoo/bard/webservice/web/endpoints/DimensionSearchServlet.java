// Copyright 2019, Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import static com.yahoo.bard.webservice.config.BardFeatureFlag.UPDATED_METADATA_COLLECTION_NAMES;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.exception.MetadataExceptionHandler;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.DimensionRequest;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.util.StreamUtils;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.ResponseFormatResolver;
import com.yahoo.bard.webservice.web.apirequest.ApiRequestImpl;
import com.yahoo.bard.webservice.web.apirequest.DimensionsApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DimensionsApiRequestImpl;
import com.yahoo.bard.webservice.web.apirequest.ResponsePaginator;
import com.yahoo.bard.webservice.web.util.PaginationParameters;
import com.yahoo.bard.webservice.data.dimension.SearchQuerySearchProvider;

import com.codahale.metrics.annotation.Timed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Resource code for dimension search resource endpoints.
 *
 * This is intended as an 'add on' to DimensionServlet and shares namespace with its endpoints.
 * Search presumes that a lookahead or similar tokenized filter needs to be made against a dimension
 * for filter building.
 */
@Singleton
@Path("dimensions/{dimensionName}/search")
public class DimensionSearchServlet extends EndpointServlet {
    private static final Logger LOG = LoggerFactory.getLogger(DimensionSearchServlet.class);

    private final DimensionDictionary dimensionDictionary;
    private final RequestMapper requestMapper;
    private final ResponseFormatResolver formatResolver;
    private final MetadataExceptionHandler exceptionHandler;

    /**
     * Constructor.
     *
     * @param dimensionDictionary  All dimensions
     * @param requestMapper  Mapper to change the API request if needed
     * @param objectMappers  JSON tools
     * @param formatResolver  The formatResolver for determining correct response format
     * @param exceptionHandler  Injection point for handling response exceptions
     */
    @Inject
    public DimensionSearchServlet(
            DimensionDictionary dimensionDictionary,
            @Named(DimensionsApiRequest.REQUEST_MAPPER_NAMESPACE) RequestMapper requestMapper,
            ObjectMappersSuite objectMappers,
            ResponseFormatResolver formatResolver,
            @Named(DimensionsApiRequest.EXCEPTION_HANDLER_NAMESPACE) MetadataExceptionHandler exceptionHandler
    ) {
        super(objectMappers);
        this.dimensionDictionary = dimensionDictionary;
        this.requestMapper = requestMapper;
        this.formatResolver = formatResolver;
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * Search endpoint. Handles text search queries against a single dimension.
     *
     * @param dimensionName  The dimension
     * @param searchQuery The query to search the dimensions with
     * @param perPage  The number of rows per page
     * @param page  The page number
     * @param format  The format of the response
     * @param downloadFilename If present, indicates the response should be downloaded by the client with the provided
     * filename. Otherwise indicates the response should be rendered in the browser.
     * @param uriInfo The injected UriInfo
     * @param containerRequestContext The injected request context
     *
     * @return A paginated list of dimensions and their metadata determined to best match the search query.
     */
    @GET
    @Timed
    public Response searchDimension(
            @PathParam("dimensionName") String dimensionName,
            @DefaultValue("") @NotNull @QueryParam("perPage") String perPage,
            @DefaultValue("") @NotNull @QueryParam("page") String page,
            @QueryParam("format") String format,
            @QueryParam("filename") String downloadFilename,
            @QueryParam("query") String searchQuery,
            @Context final UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        DimensionsApiRequest apiRequest = null;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new DimensionRequest(dimensionName, "yes"));

            apiRequest = new DimensionsApiRequestImpl(
                    dimensionName,
                    null,
                    formatResolver.apply(format, containerRequestContext),
                    downloadFilename,
                    perPage,
                    page,
                    dimensionDictionary
            );

            if (requestMapper != null) {
                apiRequest = (DimensionsApiRequestImpl) requestMapper.apply(apiRequest, containerRequestContext);
            }

            PaginationParameters paginationParameters = apiRequest
                    .getPaginationParameters()
                    .orElse(ApiRequestImpl.DEFAULT_PAGINATION);

            Pagination<DimensionRow> pagedRows;

            SearchProvider searchProvider = apiRequest.getDimension().getSearchProvider();
            if (searchProvider instanceof SearchQuerySearchProvider && !searchQuery.isEmpty()) {
                // If requested dimension's search provider CAN support search queries, perform search query.
                pagedRows = ((SearchQuerySearchProvider) searchProvider).findSearchRowsPaged(
                        searchQuery,
                        paginationParameters
                );
            } else {
                // otherwise throw error indicating to not use the search query endpoint for this dimension.
                // no message because catch clause will ignore it
                throw new UnsupportedOperationException();
            }

            Response.ResponseBuilder builder = Response.status(Response.Status.OK);

            Stream<Map<String, String>> rows = ResponsePaginator.paginate(builder, pagedRows, uriInfo)
                    .map(DimensionRow::entrySet)
                    .map(Set::stream)
                    .map(stream ->
                            stream.collect(
                                    StreamUtils.toLinkedMap(
                                            entry -> DimensionsServlet.getDescriptionKey(entry.getKey().getName()),
                                            Map.Entry::getValue
                                    )
                            )
                    );

            Response response = formatResponse(
                    apiRequest,
                    builder,
                    pagedRows,
                    containerRequestContext,
                    rows,
                    UPDATED_METADATA_COLLECTION_NAMES.isOn() ? "dimensions" : "rows",
                    null
            );

            LOG.debug("Dimension Value Endpoint Response: {}", response.getEntity());
            return response;
        } catch (UnsupportedOperationException e) {
            String msg = String.format(
                    "Dimension %s does not support search queries. Please try again using a standard api filter " +
                            "against the dimension values endpoint.",
                    apiRequest.getDimension()
            );
            LOG.debug(msg, e);
            return Response.status(Response.Status.METHOD_NOT_ALLOWED).entity(msg).build();
        } catch (Throwable t) {
            return exceptionHandler.handleThrowable(
                    t,
                    Optional.ofNullable(apiRequest),
                    containerRequestContext
            );
        } finally {
            RequestLog.stopTiming(this);
        }
    }
}
