// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import static com.yahoo.bard.webservice.config.BardFeatureFlag.UPDATED_METADATA_COLLECTION_NAMES;
import static com.yahoo.bard.webservice.web.ResponseCode.INSUFFICIENT_STORAGE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.application.metadataViews.MetadataViewProvider;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.DimensionRequest;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.util.StreamUtils;
import com.yahoo.bard.webservice.web.DimensionsApiRequest;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.ResponseFormatResolver;
import com.yahoo.bard.webservice.web.RowLimitReachedException;
import com.yahoo.bard.webservice.web.apirequest.DimensionsApiRequestImpl;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Streams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
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
    private final Map<String, MetadataViewProvider<?>> metadataBuilders;

    /**
     * Constructor.
     *
     * @param dimensionDictionary  All dimensions
     * @param logicalTableDictionary  All logical tables
     * @param requestMapper  Mapper to change the API request if needed
     * @param objectMappers  JSON tools
     * @param formatResolver  The formatResolver for determining correct response format
     */
    @Inject
    public DimensionsServlet(
            DimensionDictionary dimensionDictionary,
            LogicalTableDictionary logicalTableDictionary,
            @Named(DimensionsApiRequest.REQUEST_MAPPER_NAMESPACE) RequestMapper requestMapper,
            @Named(AbstractBinderFactory.METADATA_VIEW_PROVIDERS) Map<String, MetadataViewProvider<?>> metadataBuilders,
            ObjectMappersSuite objectMappers,
            ResponseFormatResolver formatResolver
    ) {
        super(objectMappers);
        this.dimensionDictionary = dimensionDictionary;
        this.logicalTableDictionary = logicalTableDictionary;
        this.requestMapper = requestMapper;
        this.metadataBuilders = metadataBuilders;
        this.formatResolver = formatResolver;
    }

    /**
     * Get all the dimensions as a summary list.
     *
     * @param perPage  number of values to return per page
     * @param page  the page to start from
     * @param format  the format to use for the response
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
            @Context final UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        Supplier<Response> responseSender;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new DimensionRequest("all", "no"));

            DimensionsApiRequestImpl apiRequest = new DimensionsApiRequestImpl(
                    null,
                    null,
                    formatResolver.apply(format, containerRequestContext),
                    perPage,
                    page,
                    dimensionDictionary,
                    uriInfo
            );

            if (requestMapper != null) {
                apiRequest = (DimensionsApiRequestImpl) requestMapper.apply(apiRequest, containerRequestContext);
            }

            Stream<Map<String, Object>> result = apiRequest.getPage(
                    (Set<Map<String, Object>>) apiRequest.getDimensions()
                        .stream()
                        .map(dimension ->
                                (Map<String, Object>) ((MetadataViewProvider<Dimension>) metadataBuilders
                                        .get("dimensions.summary.view"))
                                        .apply(
                                                containerRequestContext, dimension
                                        )
                        )
                        .collect(Collectors.toCollection(LinkedHashSet::new))
            );

            Response response = formatResponse(
                    apiRequest,
                    result,
                    UPDATED_METADATA_COLLECTION_NAMES.isOn() ? "dimensions" : "rows",
                    null
            );
            LOG.debug("Dimensions Endpoint Response: {}", response.getEntity());
            responseSender = () -> response;
        } catch (RequestValidationException e) {
            LOG.debug(e.getMessage(), e);
            responseSender = () -> Response.status(e.getStatus()).entity(e.getErrorHttpMsg()).build();
        } catch (Error | Exception e) {
            String msg = ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format(e.getMessage());
            LOG.error(msg, e);
            responseSender = () -> Response.status(Status.BAD_REQUEST).entity(msg).build();
        } finally {
            RequestLog.stopTiming(this);
        }

        return responseSender.get();
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
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new DimensionRequest(dimensionName, "no"));

            DimensionsApiRequestImpl apiRequest = new DimensionsApiRequestImpl(
                    dimensionName,
                    null,
                    null,
                    "",
                    "",
                    dimensionDictionary,
                    uriInfo
            );

            if (requestMapper != null) {
                apiRequest = (DimensionsApiRequestImpl) requestMapper.apply(apiRequest, containerRequestContext);
            }

            Map<String, Object> result = (Map<String, Object>) ((MetadataViewProvider<Dimension>) metadataBuilders
                    .get("dimensions.full.view"))
                    .apply(containerRequestContext, apiRequest.getDimension());

            String output = objectMappers.getMapper().writeValueAsString(result);
            LOG.debug("Dimension Endpoint Response: {}", output);
            responseSender = () -> Response.status(Status.OK).entity(output).build();
        } catch (RequestValidationException e) {
            LOG.debug(e.getMessage(), e);
            responseSender = () -> Response.status(e.getStatus()).entity(e.getErrorHttpMsg()).build();
        } catch (JsonProcessingException e) {
            String msg = ErrorMessageFormat.INTERNAL_SERVER_ERROR_ON_JSON_PROCESSING.format(e.getMessage());
            LOG.error(msg, e);
            responseSender = () -> Response.status(Status.INTERNAL_SERVER_ERROR).entity(msg).build();
        } catch (Error | Exception e) {
            String msg = ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format(e.getMessage());
            LOG.info(msg, e);
            responseSender = () -> Response.status(Status.BAD_REQUEST).entity(msg).build();
        } finally {
            RequestLog.stopTiming(this);
        }

        return responseSender.get();
    }

    /**
     * Endpoint to get values of a dimension.
     *
     * @param dimensionName  The dimension
     * @param filterQuery  The filters
     * @param page  The page number
     * @param perPage  The number of rows per page
     * @param format  The format of the response
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
            @Context final UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        Supplier<Response> responseSender;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new DimensionRequest(dimensionName, "yes"));

            DimensionsApiRequestImpl apiRequest = new DimensionsApiRequestImpl(
                    dimensionName,
                    filterQuery,
                    formatResolver.apply(format, containerRequestContext),
                    perPage,
                    page,
                    dimensionDictionary,
                    uriInfo
            );

            if (requestMapper != null) {
                apiRequest = (DimensionsApiRequestImpl) requestMapper.apply(apiRequest, containerRequestContext);
            }

            // build filtered dimension rows
            SearchProvider searchProvider = apiRequest.getDimension().getSearchProvider();
            PaginationParameters paginationParameters = apiRequest
                    .getPaginationParameters()
                    .orElse(apiRequest.getDefaultPagination());

            Pagination<DimensionRow> pagedRows = apiRequest.getFilters().isEmpty() ?
                    searchProvider.findAllDimensionRowsPaged(paginationParameters) :
                    searchProvider.findFilteredDimensionRowsPaged(
                            apiRequest.getFilters(),
                            paginationParameters
                    );

            Stream<Map<String, String>> rows = apiRequest.getPage(pagedRows)
                    .map(DimensionRow::entrySet)
                    .map(Set::stream)
                    .map(stream ->
                            stream.collect(
                                    StreamUtils.toLinkedMap(
                                            entry -> entry.getKey().getName().replace("desc", "description"),
                                            Map.Entry::getValue
                                    )
                            )
                    );

            Response response = formatResponse(
                    apiRequest,
                    rows,
                    UPDATED_METADATA_COLLECTION_NAMES.isOn() ? "dimensions" : "rows",
                    null
            );

            LOG.debug("Dimension Value Endpoint Response: {}", response.getEntity());
            responseSender = () -> response;
        } catch (RequestValidationException e) {
            LOG.debug(e.getMessage(), e);
            responseSender = () -> Response.status(e.getStatus()).entity(e.getErrorHttpMsg()).build();
        } catch (RowLimitReachedException e) {
            String msg = String.format("Row limit exceeded for dimension %s: %s", dimensionName, e.getMessage());
            LOG.debug(msg, e);
            responseSender = () -> Response.status(INSUFFICIENT_STORAGE).entity(msg).build();
        } catch (Error | Exception e) {
            String msg = ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format(e.getMessage());
            LOG.debug(msg, e);
            responseSender = () -> Response.status(BAD_REQUEST).entity(msg).build();
        } finally {
            RequestLog.stopTiming(this);
        }

        return responseSender.get();
    }

    /**
     * Get the summary list view of the dimension fields.
     *
     * @param dimensionFields  Collection of dimension fields to get the summary view for
     *
     * @return Summary list view of the dimension fields
     *
     * @deprecated should be private, now the internal usage need is gone, will deprecate in case someone is using it
     */
    @Deprecated
    public static Set<Map<String, String>> getDimensionFieldListSummaryView(
            Collection<DimensionField> dimensionFields
    ) {
        return dimensionFields.stream()
                .map(DimensionsServlet::getDimensionFieldSummaryView)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Get the summary view of the DimensionField.
     *
     * @param dimensionField  Dimension Field to get the view of
     *
     * @return Summary view of the dimension field
     *
     * @deprecated should be private, now the internal usage need is gone, will deprecate in case someone is using it
     */
    @Deprecated
    public static Map<String, String> getDimensionFieldSummaryView(DimensionField dimensionField) {
        Map<String, String> resultRow = new LinkedHashMap<>();
        resultRow.put("name", dimensionField.getName());
        resultRow.put("description", dimensionField.getDescription());
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
}
