// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.DruidQueryBuilder;
import com.yahoo.bard.webservice.data.DruidResponseParser;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.filterbuilders.DruidFilterBuilder;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQueryMerger;
import com.yahoo.bard.webservice.data.time.TimeContext;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfo;
import com.yahoo.bard.webservice.logging.blocks.DataRequest;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.Table;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.handlers.DataRequestHandler;
import com.yahoo.bard.webservice.web.handlers.RequestContext;
import com.yahoo.bard.webservice.web.handlers.RequestHandlerUtils;
import com.yahoo.bard.webservice.web.handlers.workflow.RequestWorkflowProvider;
import com.yahoo.bard.webservice.web.responseprocessors.ResultSetResponseProcessor;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

/**
 * Data Servlet responds to the data endpoint which allows for data query requests to the Druid brokers/router.
 */
@Path("data")
@Singleton
public class DataServlet extends CORSPreflightServlet {
    private static final Logger LOG = LoggerFactory.getLogger(DataServlet.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();

    private final DimensionDictionary dimensionDictionary;
    private final MetricDictionary metricDictionary;
    private final LogicalTableDictionary logicalTableDictionary;
    private final DruidQueryBuilder druidQueryBuilder;
    private final TemplateDruidQueryMerger templateDruidQueryMerger;
    private final DruidResponseParser druidResponseParser;
    private final DataRequestHandler dataRequestHandler;
    private final RequestMapper requestMapper;
    private final DruidFilterBuilder filterBuilder;

    private final TimeContext timeContext;

    private final ObjectWriter writer;
    private final ObjectMappersSuite objectMappers;

    // Default JodaTime zone to UTC
    private final DateTimeZone systemTimeZone = DateTimeZone.forID(SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("timezone"),
            "UTC"
    ));

    @Inject
    public DataServlet(
            DimensionDictionary dimensionDictionary,
            MetricDictionary metricDictionary,
            LogicalTableDictionary logicalTableDictionary,
            DruidQueryBuilder druidQueryBuilder,
            TemplateDruidQueryMerger templateDruidQueryMerger,
            DruidResponseParser druidResponseParser,
            RequestWorkflowProvider workflowProvider,
            @Named(DataApiRequest.REQUEST_MAPPER_NAMESPACE) RequestMapper requestMapper,
            ObjectMappersSuite objectMappers,
            DruidFilterBuilder filterBuilder,
            TimeContext timeContext
    ) {
        this.dimensionDictionary = dimensionDictionary;
        this.metricDictionary = metricDictionary;
        this.logicalTableDictionary = logicalTableDictionary;
        this.druidQueryBuilder = druidQueryBuilder;
        this.templateDruidQueryMerger = templateDruidQueryMerger;
        this.druidResponseParser = druidResponseParser;
        this.requestMapper = requestMapper;
        this.objectMappers = objectMappers;
        this.writer = objectMappers.getMapper().writer();
        this.dataRequestHandler = workflowProvider.buildWorkflow();
        this.filterBuilder = filterBuilder;
        this.timeContext = timeContext;

        LOG.trace(
                "Initialized with DimensionDictionary: {} \n\n" +
                        "MetricDictionary: {} \n\n" +
                        "DruidQueryBuilder: {} \n\n" +
                        "TemplateDruidQueryMerger: {} \n\n" +
                        "DruidResponseParser: {} \n\n" +
                        "DruidFilterBuilder: {}",
                this.dimensionDictionary,
                this.metricDictionary,
                this.druidQueryBuilder,
                this.templateDruidQueryMerger,
                this.druidResponseParser,
                this.filterBuilder
        );
    }

    /**
     * Logs per dimension, metric, and table meter metrics. Meters are thread safe.
     *
     * @param request  the DataApiRequest to extract the logging information from
     * @param readCache  whether cache is bypassed or not
     */
    private void logRequestMetrics(
            DataApiRequest request,
            Boolean readCache,
            DruidQuery<?> druidQuery
    ) {
        // Log dimension metrics
        Set<Dimension> dimensions = request.getDimensions();
        for (Dimension dim : dimensions) {
            REGISTRY.meter("request.dimension." + dim.getApiName()).mark();
        }

        // Log dimension metrics
        Set<LogicalMetric> metrics = request.getLogicalMetrics();
        for (LogicalMetric metric : metrics) {
            REGISTRY.meter("request.metric." + metric.getName()).mark();
        }

        // Log table metric
        Table table = request.getTable();
        REGISTRY.meter("request.logical.table." + table.getName() + "." + table.getGranularity()).mark();

        RequestLog.record(new BardQueryInfo(druidQuery.getQueryType().toJson(), false));
        RequestLog.record(
                new DataRequest(
                        table,
                        request.getIntervals(),
                        request.getFilters().values(),
                        metrics,
                        dimensions,
                        druidQuery.getDataSource().getNames(),
                        readCache,
                        request.getFormat().toString()
                )
        );
    }

    /**
     * Resource method with tableName and timeGrain as a mandatory path parameter.
     * <p>
     * The path for this method allows us to match on URLs without dimensions that don't have a trailing slash.
     *
     * @param uriInfo  UriInfo of the request
     * @param containerRequestContext  Http request context
     * @param tableName  Requested Logical TableName
     * @param timeGrain  Frequency of the request
     * @param metrics  Requested list of metrics (as a formatted string)
     * @param intervals  Requested list of intervals (as a formatted string)
     * @param filters  Requested list of filters (as a formatted string)
     * @param havings  Requested list of having queries (as a formatted string)
     * @param sorts  Requested sorting (as a formatted string)
     * @param count  Requested number of rows in the response (as a formatted string)
     * @param topN  Requested number of first rows per time bucket in the response (as a formatted string)
     * @param format  Requested format
     * @param timeZone  Requested time zone (impacts day based granularities and intervals)
     * @param perPage  Requested number of rows of data to be displayed on each page of results
     * @param page  Requested page of results desired
     * @param readCache  false to bypass cache
     * @param asyncResponse  An async response that we can use to respond asynchronously
     */
    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tableName}/{timeGrain}")
    public void getNoDimensionData(
            @Context final UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext,
            @PathParam("tableName") String tableName,
            @PathParam("timeGrain") String timeGrain,
            @QueryParam("metrics") String metrics,
            @QueryParam("dateTime") String intervals,
            @QueryParam("filters") String filters,
            @QueryParam("having") String havings,
            @QueryParam("sort") String sorts,
            @QueryParam("count") String count,
            @QueryParam("topN") String topN,
            @QueryParam("format") String format,
            @QueryParam("timeZone") String timeZone,
            @DefaultValue("") @NotNull @QueryParam("perPage") String perPage,
            @DefaultValue("") @NotNull @QueryParam("page") String page,
            @DefaultValue("true") @NotNull @QueryParam("_cache") Boolean readCache,
            @Suspended final AsyncResponse asyncResponse
    ) {
        getData(
                uriInfo,
                containerRequestContext,
                tableName,
                timeGrain,
                new ArrayList<>(),
                metrics,
                intervals,
                filters,
                havings,
                sorts,
                count,
                topN,
                format,
                timeZone,
                perPage,
                page,
                readCache,
                asyncResponse
        );
    }

    /**
     * Resource method with dimensions as a mandatory path parameter.
     *
     * @param uriInfo  UriInfo of the request
     * @param containerRequestContext  Http request context
     * @param tableName  Requested Logical TableName
     * @param timeGrain  Frequency of the request
     * @param dimensions  Requested list of metrics (as a formatted string)
     * @param metrics  Requested list of metrics (as a formatted string)
     * @param intervals  Requested list of intervals (as a formatted string)
     * @param filters  Requested list of filters (as a formatted string)
     * @param havings Requested list of havings (as a formatted string)
     * @param sorts  Requested sorting (as a formatted string)
     * @param count  Requested number of rows in the response (as a formatted string)
     * @param topN  Requested number of first rows per time bucket in the response (as a formatted string)
     * @param timeZone Requested time zone (impacts day based granularities and intervals)
     * @param format  Requested format
     * @param perPage  Requested number of rows of data to be displayed on each page of results
     * @param page  Requested page of results desired
     * @param readCache  false to bypass cache
     * @param asyncResponse  An async response that we can use to respond asynchronously
     */
    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tableName}/{timeGrain}/{dimensions:.*}")
    public void getData(
            @Context final UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext,
            @PathParam("tableName") String tableName,
            @PathParam("timeGrain") String timeGrain,
            @PathParam("dimensions") List<PathSegment> dimensions,
            @QueryParam("metrics") String metrics,
            @QueryParam("dateTime") String intervals,
            @QueryParam("filters") String filters,
            @QueryParam("having") String havings,
            @QueryParam("sort") String sorts,
            @QueryParam("count") String count,
            @QueryParam("topN") String topN,
            @QueryParam("format") String format,
            @QueryParam("timeZone") String timeZone,
            @DefaultValue("") @NotNull @QueryParam("perPage") String perPage,
            @DefaultValue("") @NotNull @QueryParam("page") String page,
            @DefaultValue("true") @NotNull @QueryParam("_cache") Boolean readCache,
            @Suspended final AsyncResponse asyncResponse
    ) {
        try {
            RequestLog.startTiming("DataApiRequest");
            DataApiRequest apiRequest = new DataApiRequest(
                    tableName,
                    timeGrain,
                    dimensions,
                    metrics,
                    intervals,
                    filters,
                    havings,
                    sorts,
                    count,
                    topN,
                    format,
                    timeZone,
                    perPage,
                    page,
                    uriInfo,
                    this
            );

            if (requestMapper != null) {
                apiRequest = (DataApiRequest) requestMapper.apply(apiRequest, containerRequestContext);
            }

            RequestLog.switchTiming("DruidQueryMerge");
            // Build the query template
            TemplateDruidQuery templateQuery = templateDruidQueryMerger.merge(apiRequest);

            RequestLog.switchTiming("DruidQueryBuilder");
            // Select the performance slice and build the final query
            DruidAggregationQuery<?> druidQuery = druidQueryBuilder.buildQuery(apiRequest, templateQuery);

            RequestLog.switchTiming("BuildRequestContext");
            // Accumulate data needed for request processing workflow
            RequestContext context = new RequestContext(containerRequestContext, readCache);
            ResultSetResponseProcessor response = new ResultSetResponseProcessor(
                    apiRequest,
                    asyncResponse,
                    druidResponseParser,
                    objectMappers
            );

            RequestLog.switchTiming("logRequestMetrics");
            logRequestMetrics(apiRequest, readCache, druidQuery);
            RequestLog.switchTiming(REQUEST_WORKFLOW_TIMER);

            // Process the request
            boolean complete = dataRequestHandler.handleRequest(context, apiRequest, druidQuery, response);
            if (! complete) {
                throw new IllegalStateException("No request handler accepted request.");
            }
        } catch (RequestValidationException e) {
            LOG.debug(e.getMessage(), e);
            RequestLog.stopMostRecentTimer();
            asyncResponse.resume(RequestHandlerUtils.makeErrorResponse(e.getStatus(), e, writer));
        } catch (Error | Exception e) {
            LOG.info("Exception processing request", e);
            RequestLog.stopMostRecentTimer();
            asyncResponse.resume(RequestHandlerUtils.makeErrorResponse(Status.BAD_REQUEST, e, writer));
        }
    }


    public DimensionDictionary getDimensionDictionary() {
        return dimensionDictionary;
    }

    public MetricDictionary getMetricDictionary() {
        return metricDictionary;
    }

    public LogicalTableDictionary getLogicalTableDictionary() {
        return logicalTableDictionary;
    }

    public DruidResponseParser getDruidResponseParser() {
        return druidResponseParser;
    }

    public DruidFilterBuilder getFilterBuilder() {
        return filterBuilder;
    }

    public ObjectWriter getWriter() {
        return writer;
    }

    public ObjectMappersSuite getObjectMappers() {
        return objectMappers;
    }

    public RequestMapper getRequestMapper() {
        return requestMapper;
    }

    public DataRequestHandler getDataRequestHandler() {
        return dataRequestHandler;
    }

    public TemplateDruidQueryMerger getTemplateDruidQueryMerger() {
        return templateDruidQueryMerger;
    }

    public DruidQueryBuilder getDruidQueryBuilder() {
        return druidQueryBuilder;
    }

    public DateTimeZone getSystemTimeZone() {
        return systemTimeZone;
    }

    public TimeContext getTimeContext() {
        return timeContext;
    }
}
