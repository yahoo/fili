// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import static com.yahoo.bard.webservice.config.BardFeatureFlag.UPDATED_METADATA_COLLECTION_NAMES;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.SliceRequest;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.ResponseFormatResolver;
import com.yahoo.bard.webservice.web.SlicesApiRequest;
import com.yahoo.bard.webservice.web.apirequest.SlicesApiRequestImpl;

import com.codahale.metrics.annotation.Timed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
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
import javax.ws.rs.core.UriInfo;

/**
 * SlicesServlet supports discovery of performance slices (aka webService PhysicalTables or druid dataSources) and their
 * current data availability intervals.  Data intervals impact partial data query routing/slice selection.
 */
@Path("/slices")
@Singleton
public class SlicesServlet extends EndpointServlet {
    private static final Logger LOG = LoggerFactory.getLogger(SlicesServlet.class);

    private final DataSourceMetadataService dataSourceMetadataService;
    private final PhysicalTableDictionary physicalTableDictionary;
    private final RequestMapper requestMapper;
    private final ResponseFormatResolver formatResolver;

    /**
     * Constructor.
     *
     * @param physicalTableDictionary  Physical Tables that this endpoint is reporting on
     * @param requestMapper  Mapper for changing the API request
     * @param dataSourceMetadataService  The data source metadata provider
     * @param objectMappers  JSON tools
     * @param formatResolver  The formatResolver for determining correct response format
     */
    @Inject
    public SlicesServlet(
            PhysicalTableDictionary physicalTableDictionary,
            @Named(SlicesApiRequest.REQUEST_MAPPER_NAMESPACE) RequestMapper requestMapper,
            DataSourceMetadataService dataSourceMetadataService,
            ObjectMappersSuite objectMappers,
            ResponseFormatResolver formatResolver
    ) {
        super(objectMappers);
        this.physicalTableDictionary = physicalTableDictionary;
        this.requestMapper = requestMapper;
        this.dataSourceMetadataService = dataSourceMetadataService;
        this.formatResolver = formatResolver;
    }

    /**
     * Endpoint to get all the performance slices.
     *
     * @param perPage  number of values to return per page
     * @param page  the page to start from
     * @param format  The name of the output format type
     * @param uriInfo  UriInfo of the request
     * @param containerRequestContext  The context of data provided by the Jersey container for this request

     * @return OK(200) else Bad Request(400) Response format:
     * <pre>
     * {@code
     * {
     *      "slices": [
     *           { "name" : "slice A", "timeGrain" : "day", "uri" : "http:/.../" },
     *           { "name" : "slice B", "timeGrain" : "day", "uri" : "http:/.../" }
     *      ]
     * }
     * }
     * </pre>
     */
    @GET
    @Timed
    public Response getSlices(
            @DefaultValue("") @NotNull @QueryParam("perPage") String perPage,
            @DefaultValue("") @NotNull @QueryParam("page") String page,
            @QueryParam("format") String format,
            @Context UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        Supplier<Response> responseSender;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new SliceRequest("all"));

            SlicesApiRequestImpl apiRequest = new SlicesApiRequestImpl(
                    null,
                    formatResolver.apply(format, containerRequestContext),
                    perPage,
                    page,
                    physicalTableDictionary,
                    dataSourceMetadataService,
                    uriInfo
            );

            if (requestMapper != null) {
                apiRequest = (SlicesApiRequestImpl) requestMapper.apply(apiRequest, containerRequestContext);
            }

            Stream<Map<String, String>> result = apiRequest.getPage(apiRequest.getSlices());

            Response response = formatResponse(
                    apiRequest,
                    result,
                    UPDATED_METADATA_COLLECTION_NAMES.isOn() ? "slices" : "rows",
                    null
            );

            LOG.debug("Slice Endpoint Response: {}", response.getEntity());
            responseSender = () -> response;
        } catch (RequestValidationException e) {
            LOG.debug(e.getMessage(), e);
            responseSender = () -> Response.status(e.getStatus()).entity(e.getErrorHttpMsg()).build();
        } catch (IOException e) {
            String msg = String.format("Internal server error. IOException : %s", e.getMessage());
            LOG.error(msg, e);
            responseSender = () -> Response.status(INTERNAL_SERVER_ERROR).entity(msg).build();
        } catch (Error | Exception e) {
            String msg = String.format("Exception processing request: %s", e.getMessage());
            LOG.info(msg, e);
            responseSender = () -> Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
        } finally {
            RequestLog.stopTiming(this);
        }

        return responseSender.get();
    }

    /**
     * Endpoint to get all the dimensions and metrics serviced by a druid slice.
     *
     * @param sliceName  Physical table name
     * @param uriInfo  UriInfo of the request
     * @param containerRequestContext  The context of data provided by the Jersey container for this request
     *
     * @return OK(200) else Bad Request(400). Response format:
     * <pre>
     * {@code
     * {
     *     "name" : "slice A",
     *     "timeGrain" : "daily",
     *     "dimensions" : [
     *         { "name" : "dimensionA", "uri" : "http://.../" , "intervals" : [ '2010-03-01/2010-10-29'.... ]},
     *         { "name" : "dimensionB", "uri" : "http://.../" , "intervals" : [ '2010-03-01/2010-10-29'.... ]}
     *     ],
     *     "metrics" : [
     *         { "name" : "metricA", "intervals" : [ '2010-03-01/2010-10-29'.... ]},
     *         { "name" : "metricB", "intervals" : [ '2010-03-01/2010-10-29'.... ]}
     *     ]
     * }
     * }
     * </pre>
     */
    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{sliceName}")
    public Response getSliceBySliceName(
            @PathParam("sliceName") String sliceName,
            @Context UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        Supplier<Response> responseSender;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new SliceRequest(sliceName));

            SlicesApiRequestImpl apiRequest = new SlicesApiRequestImpl(
                    sliceName,
                    null,
                    "",
                    "",
                    physicalTableDictionary,
                    dataSourceMetadataService,
                    uriInfo
            );

            if (requestMapper != null) {
                apiRequest = (SlicesApiRequestImpl) requestMapper.apply(apiRequest, containerRequestContext);
            }

            String output = objectMappers.getMapper().writeValueAsString(apiRequest.getSlice());
            LOG.debug("Slice Endpoint Response: {}", output);
            responseSender = () -> Response.status(Response.Status.OK).entity(output).build();
        } catch (RequestValidationException e) {
            LOG.debug(e.getMessage(), e);
            responseSender = () -> Response.status(e.getStatus()).entity(e.getErrorHttpMsg()).build();
        } catch (IOException | IllegalStateException e) {
            LOG.debug("Bad request exception : {}", e);
            responseSender = () -> Response.status(BAD_REQUEST).entity("Exception: " + e.getMessage()).build();
        } finally {
            RequestLog.stopTiming(this);
        }

        return responseSender.get();
    }

    /**
     * Get the URL of the slice.
     *
     * @param slice  The name of the slice
     * @param uriInfo  URI Info for the request
     *
     * @return The absolute URL for the slice
     */
    public static String getSliceDetailUrl(String slice, UriInfo uriInfo) {
        return uriInfo.getBaseUriBuilder()
                .path(SlicesServlet.class)
                .path(SlicesServlet.class, "getSliceBySliceName")
                .build(slice)
                .toASCIIString();
    }
}
