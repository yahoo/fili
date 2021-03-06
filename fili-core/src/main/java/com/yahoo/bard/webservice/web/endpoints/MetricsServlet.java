// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import static com.yahoo.bard.webservice.config.BardFeatureFlag.UPDATED_METADATA_COLLECTION_NAMES;
import static com.yahoo.bard.webservice.web.endpoints.views.DefaultMetadataViewFormatters.metricMetadataFormatter;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.exception.MetadataExceptionHandler;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.MetricRequest;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.web.MetadataObject;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.ResponseFormatResolver;
import com.yahoo.bard.webservice.web.apirequest.MetricsApiRequest;
import com.yahoo.bard.webservice.web.apirequest.MetricsApiRequestImpl;
import com.yahoo.bard.webservice.web.endpoints.views.MetricMetadataFormatter;

import com.codahale.metrics.annotation.Timed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Supplier;

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
 * Resource code for metric resource endpoints.
 */
@Path("/metrics")
@Singleton
public class MetricsServlet extends EndpointServlet {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsServlet.class);

    private final MetricDictionary metricDictionary;
    private final LogicalTableDictionary logicalTableDictionary;
    private final RequestMapper requestMapper;
    private final ResponseFormatResolver formatResolver;
    private final MetadataExceptionHandler exceptionHandler;

    /**
     * Constructor.
     *
     * @param metricDictionary  Metrics to know about
     * @param logicalTableDictionary  Logical tables to know about
     * @param requestMapper  Mapper to change the API request if needed
     * @param objectMappers  JSON tools
     * @param formatResolver  The formatResolver for determining correct response format
     * @param exceptionHandler  Injection point for handling response exceptions
     */
    @Inject
    public MetricsServlet(
            MetricDictionary metricDictionary,
            LogicalTableDictionary logicalTableDictionary,
            @Named(MetricsApiRequest.REQUEST_MAPPER_NAMESPACE) RequestMapper requestMapper,
            ObjectMappersSuite objectMappers,
            ResponseFormatResolver formatResolver,
            @Named(MetricsApiRequest.EXCEPTION_HANDLER_NAMESPACE) MetadataExceptionHandler exceptionHandler
    ) {
        super(objectMappers);
        this.metricDictionary = metricDictionary;
        this.logicalTableDictionary = logicalTableDictionary;
        this.requestMapper = requestMapper;
        this.formatResolver = formatResolver;
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * Get the URL of the logical metric.
     *
     * @param logicalMetric  Logical metric to get the URL of
     * @param uriInfo  URI Info for the request
     *
     * @return The absolute URL for the logical metric
     */
    public static String getLogicalMetricUrl(LogicalMetric logicalMetric, UriInfo uriInfo) {
        return uriInfo.getBaseUriBuilder()
                .path(MetricsServlet.class)
                .path(MetricsServlet.class, "getMetric")
                .build(logicalMetric.getName())
                .toASCIIString();
    }

    /**
     * Get all the logical metrics as a summary list.
     *
     * @param perPage  number of values to return per page
     * @param page  the page to start from
     * @param format  The name of the output format type
     * @param downloadFilename If present, indicates the response should be downloaded by the client with the provided
     * filename. Otherwise indicates the response should be rendered in the browser.
     * @param containerRequestContext  The context of data provided by the Jersey container for this request
     *
     * @return The list of logical metrics
     * <p>
     * {@code
     * {
     *     "metrics": <List of Metric Summaries>
     * }
     * }
     * @see MetricMetadataFormatter#formatMetricSummaryList(Collection, UriInfo)
     */
    @GET
    @Timed
    public Response getAllLogicalMetrics(
            @DefaultValue("") @NotNull @QueryParam("perPage") String perPage,
            @DefaultValue("") @NotNull @QueryParam("page") String page,
            @QueryParam("format") String format,
            @QueryParam("filename") String downloadFilename,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        UriInfo uriInfo = containerRequestContext.getUriInfo();
        MetricsApiRequest apiRequest = null;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new MetricRequest("all"));

            apiRequest = new MetricsApiRequestImpl(
                    null,
                    formatResolver.apply(format, containerRequestContext),
                    downloadFilename,
                    perPage,
                    page,
                    metricDictionary
            );

            if (requestMapper != null) {
                apiRequest = (MetricsApiRequestImpl) requestMapper.apply(apiRequest, containerRequestContext);
            }

            Response response = paginateAndFormatResponse(
                    apiRequest,
                    containerRequestContext,
                    metricMetadataFormatter.formatMetricSummaryList(apiRequest.getMetrics(), uriInfo),
                    UPDATED_METADATA_COLLECTION_NAMES.isOn() ? "metrics" : "rows",
                    null
            );
            LOG.trace("Metrics Endpoint Response: {}", response.getEntity());
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
     * Get the details of a specific logical metric.
     *
     * @param metricName  Logical metric name
     * @param format The format for the response (e.g. json, csv)
     * @param containerRequestContext  The context of data provided by the Jersey container for this request
     *
     * @return The logical metric
     * @see MetricMetadataFormatter#formatLogicalMetricWithJoins(LogicalMetric, LogicalTableDictionary, UriInfo)
     */
    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{metricName}")
    public Response getMetric(
            @PathParam("metricName") String metricName,
            @PathParam("format") String format,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        UriInfo uriInfo = containerRequestContext.getUriInfo();
        Supplier<Response> responseSender;
        MetricsApiRequest apiRequest = null;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new MetricRequest(metricName));

            apiRequest = new MetricsApiRequestImpl(
                    metricName,
                    format,
                    null,
                    "",
                    "",
                    metricDictionary
            );

            if (requestMapper != null) {
                apiRequest = (MetricsApiRequest) requestMapper.apply(apiRequest, containerRequestContext);
            }

            MetadataObject result = metricMetadataFormatter.formatLogicalMetricWithJoins(
                    apiRequest.getMetric(),
                    logicalTableDictionary,
                    uriInfo
            );

            String output = objectMappers.getMapper().writeValueAsString(result);
            LOG.trace("Metric Endpoint Response: {}", output);
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
}
