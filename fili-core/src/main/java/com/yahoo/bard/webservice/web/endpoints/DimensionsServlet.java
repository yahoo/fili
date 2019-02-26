// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import static com.yahoo.bard.webservice.config.BardFeatureFlag.UPDATED_METADATA_COLLECTION_NAMES;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.exception.MetadataExceptionHandler;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.DimensionRequest;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.util.StreamUtils;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.ResponseFormatResolver;
import com.yahoo.bard.webservice.web.apirequest.ApiRequestImpl;
import com.yahoo.bard.webservice.web.apirequest.DimensionsApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DimensionsApiRequestImpl;
import com.yahoo.bard.webservice.web.apirequest.ResponsePaginator;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Streams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

/**
 * Resource code for dimension resource endpoints.
 */
@Path("/dimensions")
@Singleton
public class DimensionsServlet extends EndpointServlet {
    private static final Logger LOG = LoggerFactory.getLogger(DimensionsServlet.class);

    private final DimensionDictionary dimensionDictionary;
    private final LogicalTableDictionary logicalTableDictionary;
    private final RequestMapper requestMapper;
    private final ResponseFormatResolver formatResolver;
    private final MetadataExceptionHandler exceptionHandler;

    /**
     * Constructor.
     *
     * @param dimensionDictionary  All dimensions
     * @param logicalTableDictionary  All logical tables
     * @param requestMapper  Mapper to change the API request if needed
     * @param objectMappers  JSON tools
     * @param formatResolver  The formatResolver for determining correct response format
     * @param exceptionHandler  Injection point for handling response exceptions
     */
    @Inject
    public DimensionsServlet(
            DimensionDictionary dimensionDictionary,
            LogicalTableDictionary logicalTableDictionary,
            @Named(DimensionsApiRequest.REQUEST_MAPPER_NAMESPACE) RequestMapper requestMapper,
            ObjectMappersSuite objectMappers,
            ResponseFormatResolver formatResolver,
            @Named(DimensionsApiRequest.EXCEPTION_HANDLER_NAMESPACE) MetadataExceptionHandler exceptionHandler
    ) {
        super(objectMappers);
        this.dimensionDictionary = dimensionDictionary;
        this.logicalTableDictionary = logicalTableDictionary;
        this.requestMapper = requestMapper;
        this.formatResolver = formatResolver;
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * Get all the dimensions as a summary list.
     *
     * @param perPage  number of values to return per page
     * @param page  the page to start from
     * @param format  the format to use for the response
     * @param downloadFilename If present, indicates the response should be downloaded by the client with the provided
     * username. Otherwise indicates the response should be rendered in the browser.
     * @param uriInfo  UriInfo of the request
     * @param containerRequestContext  The context of data provided by the Jersey container for this request
     *
     * @return The list of dimensions
     * <pre><code>
     * {
     *     "dimensions": {@literal <List of Dimension Summaries>}
     * }
     * </code></pre>
     * @see DimensionsServlet#getDimensionListSummaryView(Iterable, UriInfo)
     */
    @GET
    @Timed
    public Response getAllDimensions(
            @DefaultValue("") @NotNull @QueryParam("perPage") String perPage,
            @DefaultValue("") @NotNull @QueryParam("page") String page,
            @QueryParam("format") String format,
            @QueryParam("filename") String downloadFilename,
            @Context final UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        DimensionsApiRequest apiRequest = null;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new DimensionRequest("all", "no"));

            apiRequest = new DimensionsApiRequestImpl(
                    null,
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

            Response response = paginateAndFormatResponse(
                    apiRequest,
                    containerRequestContext,
                    getDimensionListSummaryView(apiRequest.getDimensions(), uriInfo),
                    UPDATED_METADATA_COLLECTION_NAMES.isOn() ? "dimensions" : "rows",
                    null
            );
            LOG.trace("Dimensions Endpoint Response: {}", response.getEntity());
            return response;
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

    /**
     * Get the details of a dimension.
     *
     * @param dimensionName  Dimension to get the details of
     * @param uriInfo  UriInfo of the request
     * @param containerRequestContext  The context of data provided by the Jersey container for this request
     *
     * @return Full view of the dimension
     * @see DimensionsServlet#getDimensionFullView(Dimension, LogicalTableDictionary, UriInfo)
     */
    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{dimensionName}")
    public Response getDimension(
            @PathParam("dimensionName") String dimensionName,
            @Context final UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        Supplier<Response> responseSender;
        DimensionsApiRequest apiRequest = null;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new DimensionRequest(dimensionName, "no"));

            apiRequest = new DimensionsApiRequestImpl(
                    dimensionName,
                    null,
                    null,
                    null,
                    "",
                    "",
                    dimensionDictionary
            );

            if (requestMapper != null) {
                apiRequest = (DimensionsApiRequestImpl) requestMapper.apply(apiRequest, containerRequestContext);
            }

            Map<String, Object> result = getDimensionFullView(
                    apiRequest.getDimension(),
                    logicalTableDictionary,
                    uriInfo
            );

            String output = objectMappers.getMapper().writeValueAsString(result);
            LOG.trace("Dimension Endpoint Response: {}", output);
            responseSender = () -> Response.status(Status.OK).entity(output).build();
        } catch (Throwable t) {
            return exceptionHandler.handleThrowable(
                    t,
                    Optional.ofNullable(apiRequest),
                    containerRequestContext
            );
        } finally {
            RequestLog.stopTiming(this);
        }

        return responseSender.get();
    }

    /**
     * Endpoint to get values of a dimension.
     *
     * This implementation calls an overridable method {@link #getPagedRows(DimensionsApiRequest, SearchProvider,
     * PaginationParameters)}, which fetch pagination of dimension rows.
     *
     *
     * @param dimensionName  The dimension
     * @param filterQuery  The filters
     * @param page  The page number
     * @param perPage  The number of rows per page
     * @param format  The format of the response
     * @param downloadFilename If present, indicates the response should be downloaded by the client with the provided
     * filename. Otherwise indicates the response should be rendered in the browser.
     * @param uriInfo The injected UriInfo
     * @param containerRequestContext The injected request context
     *
     * @return OK(200) else Bad Request(400). Response format: JSON or CSV JSON Response:
     * <pre><code>
     * JSON:
     * {
     *     "rows" : [
     *             { "id" : 1234, "description" : "description of dimension value" },
     *             { "id" : 1235, "description" : "another description" }
     *     ],
     *     "next" : "http://{@literal <next_page_uri>}/,
     *     "previous": "http://{@literal <previous_page_uri>}/
     * }
     *
     * CSV:
     *     id, description
     *     1234, description of dimension value
     *     1235, another description
     *     ...
     * </code></pre>
     */
    @GET
    @Timed
    @Path("/{dimensionName}/values")
    public Response getDimensionRows(
            @PathParam("dimensionName") String dimensionName,
            @QueryParam("filters") String filterQuery,
            @DefaultValue("") @NotNull @QueryParam("perPage") String perPage,
            @DefaultValue("") @NotNull @QueryParam("page") String page,
            @QueryParam("format") String format,
            @QueryParam("filename") String downloadFilename,
            @Context final UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        Supplier<Response> responseSender;
        DimensionsApiRequest apiRequest = null;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new DimensionRequest(dimensionName, "yes"));

            apiRequest = new DimensionsApiRequestImpl(
                    dimensionName,
                    filterQuery,
                    formatResolver.apply(format, containerRequestContext),
                    downloadFilename,
                    perPage,
                    page,
                    dimensionDictionary
            );

            if (requestMapper != null) {
                apiRequest = (DimensionsApiRequestImpl) requestMapper.apply(apiRequest, containerRequestContext);
            }

            // build filtered dimension rows
            SearchProvider searchProvider = apiRequest.getDimension().getSearchProvider();
            PaginationParameters paginationParameters = apiRequest
                    .getPaginationParameters()
                    .orElse(ApiRequestImpl.DEFAULT_PAGINATION);

            Pagination<DimensionRow> pagedRows = getPagedRows(apiRequest, searchProvider, paginationParameters);
            Response.ResponseBuilder builder = Response.status(Response.Status.OK);

            Stream<Map<String, String>> rows = ResponsePaginator.paginate(builder, pagedRows, uriInfo)
                    .map(DimensionRow::entrySet)
                    .map(Set::stream)
                    .map(stream ->
                            stream.collect(
                                    StreamUtils.toLinkedMap(
                                            entry -> getDescriptionKey(entry.getKey().getName()),
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

            LOG.trace("Dimension Value Endpoint Response: {}", response.getEntity());
            return response;
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

    /***
     * Get the pagination of dimension rows.
     * <p>
     * if there is filter in API request, then apply the filter to the querying dimension rows, otherwise returns all
     * dimension rows.
     *
     * @param apiRequest  The apiRequest
     * @param searchProvider  The searchProvider
     * @param paginationParameters  The pagination parameters
     *
     * @return Pagination of dimensionRow
     */
    protected Pagination<DimensionRow> getPagedRows(
            DimensionsApiRequest apiRequest,
            SearchProvider searchProvider,
            PaginationParameters paginationParameters
    ) {
        return apiRequest.getFilters().isEmpty() ?
                searchProvider.findAllDimensionRowsPaged(paginationParameters) :
                searchProvider.findFilteredDimensionRowsPaged(
                        apiRequest.getFilters(),
                        paginationParameters
                );
    }

    /**
     * Get the summary list view of the dimensions.
     *
     * @param dimensions  Collection of dimensions to get the summary view for
     * @param uriInfo  UriInfo of the request
     *
     * @return Summary list view of the dimensions
     */
    public static LinkedHashSet<Map<String, Object>> getDimensionListSummaryView(
            Iterable<Dimension> dimensions,
            final UriInfo uriInfo
    ) {
        return Streams.stream(dimensions)
                .map(dimension -> getDimensionSummaryView(dimension, uriInfo))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Get the summary view of the dimension.
     *
     * @param dimension  Dimension to get the view of
     * @param uriInfo  UriInfo of the request
     *
     * @return Summary view of the dimension
     */
    public static Map<String, Object> getDimensionSummaryView(Dimension dimension, final UriInfo uriInfo) {
        Map<String, Object> resultRow = new LinkedHashMap<>();
        resultRow.put("category", dimension.getCategory());
        resultRow.put("name", dimension.getApiName());
        resultRow.put("longName", dimension.getLongName());
        resultRow.put("uri", getDimensionUrl(dimension, uriInfo));
        resultRow.put("cardinality", dimension.getCardinality());
        resultRow.put("storageStrategy", dimension.getStorageStrategy());
        return resultRow;
    }

    /**
     * Get the full view of the dimension.
     *
     * @param dimension  Dimension to get the view of
     * @param logicalTableDictionary  Logical Table Dictionary to look up the logical tables this dimension is on
     * @param uriInfo  UriInfo of the request
     *
     * @return Full view of the dimension
     */
    public static Map<String, Object> getDimensionFullView(
            Dimension dimension,
            LogicalTableDictionary logicalTableDictionary,
            final UriInfo uriInfo
    ) {
        Map<String, Object> resultRow = new LinkedHashMap<>();
        resultRow.put("category", dimension.getCategory());
        resultRow.put("name", dimension.getApiName());
        resultRow.put("longName", dimension.getLongName());
        resultRow.put("description", dimension.getDescription());
        resultRow.put("fields", dimension.getDimensionFields());
        resultRow.put("values", getDimensionValuesUrl(dimension, uriInfo));
        resultRow.put("cardinality", dimension.getCardinality());
        resultRow.put("storageStrategy", dimension.getStorageStrategy());
        resultRow.put(
                "tables",
                TablesServlet.getLogicalTableListSummaryView(
                        logicalTableDictionary.findByDimension(dimension),
                        uriInfo
                )
        );
        return resultRow;
    }

    /**
     * Get the URL of the dimension.
     *
     * @param dimension  Dimension to get the URL of
     * @param uriInfo  URI Info for the request
     *
     * @return The absolute URL for the dimension
     */
    public static String getDimensionUrl(Dimension dimension, final UriInfo uriInfo) {
        return uriInfo.getBaseUriBuilder()
                .path(DimensionsServlet.class)
                .path(DimensionsServlet.class, "getDimension")
                .build(dimension.getApiName())
                .toASCIIString();
    }

    /**
     * Get the URL of the dimension values collection.
     *
     * @param dimension  Dimension to get the URL of
     * @param uriInfo  URI Info for the request
     *
     * @return The absolute URL for the dimension
     */
    public static String getDimensionValuesUrl(Dimension dimension, final UriInfo uriInfo) {
        return uriInfo.getBaseUriBuilder()
                .path(DimensionsServlet.class)
                .path(DimensionsServlet.class, "getDimensionRows")
                .build(dimension.getApiName())
                .toASCIIString();
    }

    /**
     * If a description dimension fields has a name of "desc", transforms it to "description", otherwise returns the
     * original without modification.
     * <p>
     * TODO: This rewrite need to be removed once description is normalized in legacy implementations, see
     * {@link com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension#parseDimensionRow(Map)}.
     *
     * @param fieldName  The name of the description field name
     *
     * @return a description dimension field with name "description"
     */
    public static String getDescriptionKey(String fieldName) {
        return fieldName.contains("description") ? fieldName : fieldName.replace("desc", "description");
    }
}
