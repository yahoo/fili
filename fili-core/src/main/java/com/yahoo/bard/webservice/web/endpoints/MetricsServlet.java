// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import static com.yahoo.bard.webservice.config.BardFeatureFlag.UPDATED_METADATA_COLLECTION_NAMES;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.application.metadataViews.MetadataViewProvider;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.MetricRequest;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.MetricsApiRequest;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.ResponseFormatResolver;
import com.yahoo.bard.webservice.web.apirequest.MetricsApiRequestImpl;

import com.codahale.metrics.annotation.Timed;

import org.hibernate.validator.internal.metadata.provider.MetaDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.rmi.runtime.Log;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collector;
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
 * Resource code for metric resource endpoints.
 */
@Path("/metrics")
@Singleton
public class MetricsServlet extends EndpointServlet {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsServlet.class);

    private final MetricDictionary metricDictionary;
    private final RequestMapper requestMapper;
    private final ResponseFormatResolver formatResolver;
    private final Map<String, MetadataViewProvider<?>> metadataBuilders;

    /**
     * Constructor.
     *
     * @param metricDictionary  Metrics to know about
     * @param logicalTableDictionary  Logical tables to know about
     * @param requestMapper  Mapper to change the API request if needed
     * @param objectMappers  JSON tools
     * @param formatResolver  The formatResolver for determining correct response format
     */
    @Inject
    public MetricsServlet(
            MetricDictionary metricDictionary,
            @Named(MetricsApiRequest.REQUEST_MAPPER_NAMESPACE) RequestMapper requestMapper,
            @Named(AbstractBinderFactory.METADATA_VIEW_PROVIDERS) Map<String, MetadataViewProvider<?>> metadataBuilders,
            ObjectMappersSuite objectMappers,
            ResponseFormatResolver formatResolver
    ) {
        super(objectMappers);
        this.metricDictionary = metricDictionary;
        this.requestMapper = requestMapper;
        this.formatResolver = formatResolver;
        this.metadataBuilders = metadataBuilders;
    }

    /**
     * Get all the logical metrics as a summary list.
     *
     * @param perPage  number of values to return per page
     * @param page  the page to start from
     * @param format  The name of the output format type
     * @param uriInfo  UriInfo of the request
     * @param containerRequestContext  The context of data provided by the Jersey container for this request
     *
     * @return The list of logical metrics
     * <p>
     * {@code
     * {
     *     "metrics": <List of Metric Summaries>
     * }
     * }
     * @see MetricsServlet#getLogicalMetricListSummaryView(Collection, UriInfo)
     */
    @GET
    @Timed
    public Response getAllLogicalMetrics(
            @DefaultValue("") @NotNull @QueryParam("perPage") String perPage,
            @DefaultValue("") @NotNull @QueryParam("page") String page,
            @QueryParam("format") String format,
            @Context UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        Supplier<Response> responseSender;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new MetricRequest("all"));

            MetricsApiRequestImpl apiRequest = new MetricsApiRequestImpl(
                    null,
                    formatResolver.apply(format, containerRequestContext),
                    perPage,
                    page,
                    metricDictionary,
                    uriInfo
            );

            if (requestMapper != null) {
                apiRequest = (MetricsApiRequestImpl) requestMapper.apply(apiRequest, containerRequestContext);
            }

            Stream<Map<String, String>> result = apiRequest.getPage(
                    (Set<Map<String, String>>) apiRequest.getMetrics()
                            .stream()
                            .map(logicalMetric ->
                                    (Map<String, String>) ((MetadataViewProvider<LogicalMetric>) metadataBuilders
                                            .get("metrics.summary.view"))
                                            .apply(containerRequestContext, logicalMetric)
                            )
                            .collect(Collectors.toCollection(LinkedHashSet::new))
            );

            Response response = formatResponse(
                    apiRequest,
                    result,
                    UPDATED_METADATA_COLLECTION_NAMES.isOn() ? "metrics" : "rows",
                    null
            );
            LOG.debug("Metrics Endpoint Response: {}", response.getEntity());
            responseSender = () -> response;
        } catch (RequestValidationException e) {
            LOG.debug(e.getMessage(), e);
            responseSender = () -> Response.status(e.getStatus()).entity(e.getErrorHttpMsg()).build();
        } catch (IOException e) {
            String msg = String.format("Internal server error. IOException : %s", e.getMessage());
            LOG.error(msg, e);
            responseSender = () -> Response.status(INTERNAL_SERVER_ERROR).entity(msg).build();
        } catch (Error | Exception e) {
            String msg = ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format(e.getMessage());
            LOG.info(msg, e);
            responseSender = () -> Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
        } finally {
            RequestLog.stopTiming(this);
        }

        return responseSender.get();
    }

    /**
     * Get the details of a specific logical metric.
     *
     * @param metricName  Logical metric name
     * @param uriInfo  UriInfo of the request
     * @param containerRequestContext  The context of data provided by the Jersey container for this request
     *
     * @return The logical metric
     * @see MetricsServlet#getLogicalMetricFullView(LogicalMetric, LogicalTableDictionary, UriInfo)
     */
    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{metricName}")
    public Response getMetric(
            @PathParam("metricName") String metricName,
            @Context UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        Supplier<Response> responseSender;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new MetricRequest(metricName));

            MetricsApiRequest apiRequest = new MetricsApiRequestImpl(
                    metricName,
                    null,
                    "",
                    "",
                    metricDictionary,
                    uriInfo
            );

            if (requestMapper != null) {
                apiRequest = (MetricsApiRequest) requestMapper.apply(apiRequest, containerRequestContext);
            }

            Map<String, Object> result =
                    (Map<String, Object>) ((MetadataViewProvider<LogicalMetric>) metadataBuilders
                            .get("metrics.view"))
                            .apply(
                                containerRequestContext,
                                apiRequest.getMetric()
                            );

            String output = objectMappers.getMapper().writeValueAsString(result);
            LOG.debug("Metric Endpoint Response: {}", output);
            responseSender = () -> Response.status(Status.OK).entity(output).build();
        } catch (RequestValidationException e) {
            LOG.debug(e.getMessage(), e);
            responseSender = () -> Response.status(e.getStatus()).entity(e.getErrorHttpMsg()).build();
        } catch (IOException e) {
            String msg = String.format("Internal server error. IOException : %s", e.getMessage());
            LOG.error(msg, e);
            responseSender = () -> Response.status(INTERNAL_SERVER_ERROR).entity(msg).build();
        } catch (Error | Exception e) {
            String msg = ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format(e.getMessage());
            LOG.info(msg, e);
            responseSender = () -> Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
        } finally {
            RequestLog.stopTiming(this);
        }

        return responseSender.get();
    }

    /**
     * Get the summary list view of the logical metrics.
     *
     * @param logicalMetrics  Collection of logical metrics to get the summary view for
     * @param uriInfo  UriInfo of the request
     *
     * @return Summary list view of the logical metrics
     */
    private Set<Map<String, String>> getLogicalMetricListSummaryView(
            Collection<LogicalMetric> logicalMetrics,
            ContainerRequestContext containerRequestContext,
            MetadataViewProvider<LogicalMetric> metricMetadataViewProvider
    ) {
        return logicalMetrics.stream()
                .map(logicalMetric ->
                        (Map<String, String>) metricMetadataViewProvider.apply(
                                containerRequestContext,
                                logicalMetric)
                )
                .collect(Collectors.toCollection(LinkedHashSet::new));
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
}
