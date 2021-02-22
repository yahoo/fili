// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.async.MetadataHttpResponseChannel;
import com.yahoo.bard.webservice.async.broadcastchannels.BroadcastChannel;
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow;
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRowBuilder;
import com.yahoo.bard.webservice.async.jobs.payloads.JobPayloadBuilder;
import com.yahoo.bard.webservice.async.workflows.AsynchronousWorkflows;
import com.yahoo.bard.webservice.async.workflows.AsynchronousWorkflowsBuilder;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.DruidQueryBuilder;
import com.yahoo.bard.webservice.data.DruidResponseParser;
import com.yahoo.bard.webservice.data.HttpResponseChannel;
import com.yahoo.bard.webservice.data.HttpResponseMaker;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQueryMerger;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.exception.DataExceptionHandler;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.logging.blocks.BardQueryInfo;
import com.yahoo.bard.webservice.logging.blocks.DataRequest;
import com.yahoo.bard.webservice.logging.blocks.DruidFilterInfo;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.util.Either;
import com.yahoo.bard.webservice.web.PreResponse;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.ResponseFormatResolver;
import com.yahoo.bard.webservice.web.apirequest.ApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestFactory;
import com.yahoo.bard.webservice.web.apirequest.generator.LegacyGenerator;
import com.yahoo.bard.webservice.web.apirequest.generator.having.HavingGenerator;
import com.yahoo.bard.webservice.web.apirequest.generator.metric.ApiRequestLogicalMetricBinder;
import com.yahoo.bard.webservice.web.handlers.DataRequestHandler;
import com.yahoo.bard.webservice.web.handlers.RequestContext;
import com.yahoo.bard.webservice.web.handlers.RequestHandlerUtils;
import com.yahoo.bard.webservice.web.handlers.workflow.RequestWorkflowProvider;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessor;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseProcessorFactory;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.observables.ConnectableObservable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Data Servlet responds to the data endpoint which allows for data query requests to the Druid brokers/router.
 */
@Path("data")
@Singleton
public class DataServlet extends CORSPreflightServlet implements BardConfigResources {
    private static final Logger LOG = LoggerFactory.getLogger(DataServlet.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();

    private final ResourceDictionaries resourceDictionaries;
    private final DruidQueryBuilder druidQueryBuilder;
    private final TemplateDruidQueryMerger templateDruidQueryMerger;
    private final DruidResponseParser druidResponseParser;
    private final DataRequestHandler dataRequestHandler;
    private final RequestMapper requestMapper;
    private final DruidFilterBuilder filterBuilder;
    private final HavingGenerator havingGenerator;
    private final JobRowBuilder jobRowBuilder;
    private final AsynchronousWorkflowsBuilder asynchronousWorkflowsBuilder;
    private final JobPayloadBuilder jobPayloadBuilder;
    private final BroadcastChannel<String> preResponseStoredNotifications;
    private final DateTimeFormatter dateTimeFormatter;

    private final ApiRequestLogicalMetricBinder metricBinder;
    private final LegacyGenerator<List<OrderByColumn>> orderByGenerator;

    private final GranularityParser granularityParser;

    private final ObjectWriter writer;
    private final ObjectMappersSuite objectMappers;
    private final HttpResponseMaker httpResponseMaker;
    private final ResponseFormatResolver formatResolver;
    private final ResponseProcessorFactory responseProcessorFactory;

    private final DataApiRequestFactory dataApiRequestFactory;


    // Default JodaTime zone to UTC
    private final DateTimeZone systemTimeZone = DateTimeZone.forID(SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("timezone"),
            "UTC"
    ));

    private final DataExceptionHandler exceptionHandler;

    /**
     * Constructor.
     * @param resourceDictionaries  Dictionary holder
     * @param druidQueryBuilder  A builder for converting API Requests into Druid Queries
     * @param templateDruidQueryMerger  A helper to merge TemplateDruidQueries together
     * @param druidResponseParser  Parses Druid responses
     * @param workflowProvider  Provides the static workflow for the system
     * @param requestMapper  Allows for overriding the API request
     * @param objectMappers  JSON serialization tools
     * @param filterBuilder  Helper to build filters
     * @param havingGenerator  Helper to generate having
     * @param granularityParser  Helper for parsing granularities
     * @param jobPayloadBuilder  The factory for building a view of the JobRow that is sent to the user
     * @param jobRowBuilder  The JobRows factory
     * @param asynchronousWorkflowsBuilder  The factory for building the asynchronous workflow
     * @param preResponseStoredNotifications  The broadcast channel responsible for notifying other Bard processes
     * @param httpResponseMaker  The factory for building HTTP responses
* that a query has been completed and its results stored in the
     * @param formatResolver  The formatResolver for determining correct response format
     * @param dataApiRequestFactory A factory to build dataApiRequests
* {@link com.yahoo.bard.webservice.async.preresponses.stores.PreResponseStore}
     * @param responseProcessorFactory  Builds the object that performs post processing on a Druid response
     * @param exceptionHandler  Injects custom logic for handling exceptions thrown during request processing
     * @param dateTimeFormatter  date time formatter
     * @param metricBinder  A binder to build logical metrics
     * @param orderByGenerator  A factory to bind and validate sort clauses
     */
    @Inject
    public DataServlet(
            ResourceDictionaries resourceDictionaries,
            DruidQueryBuilder druidQueryBuilder,
            TemplateDruidQueryMerger templateDruidQueryMerger,
            DruidResponseParser druidResponseParser,
            RequestWorkflowProvider workflowProvider,
            @Named(DataApiRequest.REQUEST_MAPPER_NAMESPACE) RequestMapper requestMapper,
            ObjectMappersSuite objectMappers,
            DruidFilterBuilder filterBuilder,
            HavingGenerator havingGenerator,
            GranularityParser granularityParser,
            JobPayloadBuilder jobPayloadBuilder,
            JobRowBuilder jobRowBuilder,
            AsynchronousWorkflowsBuilder asynchronousWorkflowsBuilder,
            BroadcastChannel<String> preResponseStoredNotifications,
            HttpResponseMaker httpResponseMaker,
            ResponseFormatResolver formatResolver,
            DataApiRequestFactory dataApiRequestFactory,
            ResponseProcessorFactory responseProcessorFactory,
            DataExceptionHandler exceptionHandler,
            DateTimeFormatter dateTimeFormatter,
            ApiRequestLogicalMetricBinder metricBinder,
            @Named(DataApiRequest.ORDER_BY_GENERATOR_NAMESPACE) LegacyGenerator<List<OrderByColumn>> orderByGenerator
    ) {
        this.resourceDictionaries = resourceDictionaries;
        this.druidQueryBuilder = druidQueryBuilder;
        this.templateDruidQueryMerger = templateDruidQueryMerger;
        this.druidResponseParser = druidResponseParser;
        this.requestMapper = requestMapper;
        this.objectMappers = objectMappers;
        this.writer = objectMappers.getMapper().writer();
        this.dataRequestHandler = workflowProvider.buildWorkflow();
        this.filterBuilder = filterBuilder;
        this.havingGenerator = havingGenerator;
        this.granularityParser = granularityParser;
        this.jobPayloadBuilder = jobPayloadBuilder;
        this.jobRowBuilder = jobRowBuilder;
        this.asynchronousWorkflowsBuilder = asynchronousWorkflowsBuilder;
        this.preResponseStoredNotifications = preResponseStoredNotifications;
        this.httpResponseMaker = httpResponseMaker;
        this.formatResolver = formatResolver;
        this.dataApiRequestFactory = dataApiRequestFactory;
        this.responseProcessorFactory = responseProcessorFactory;
        this.exceptionHandler = exceptionHandler;
        this.dateTimeFormatter = dateTimeFormatter;
        this.metricBinder = metricBinder;
        this.orderByGenerator = orderByGenerator;

        LOG.trace(
                "Initialized with ResourceDictionaries: {} \n\n" +
                        "DruidQueryBuilder: {} \n\n" +
                        "TemplateDruidQueryMerger: {} \n\n" +
                        "DruidResponseParser: {} \n\n" +
                        "DruidFilterBuilder: {}",
                this.resourceDictionaries,
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
     * @param druidQuery  Druid query for which we're logging metrics
     * @param templateDruidQuery  Template druid query for which we're logging metrics
     */
    private void logRequestMetrics(
            DataApiRequest request,
            Boolean readCache,
            DruidQuery<?> druidQuery,
            TemplateDruidQuery templateDruidQuery
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
        LogicalTable table = request.getTable();
        REGISTRY.meter("request.logical.table." + table.getName() + "." + table.getGranularity()).mark();

        RequestLog.record(new BardQueryInfo(druidQuery.getQueryType().toJson()));
        RequestLog.record(
                new DataRequest(
                        table,
                        request.getIntervals(),
                        request.getApiFilters().values(),
                        metrics,
                        dimensions,
                        templateDruidQuery.getMetricDimensions(),
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
     * @param asyncAfter  The maximum time length (in milliseconds) a request is allowed to be synchronous before
     * becoming asynchronous, if "never" then the request is forever synchronous
     * @param downloadFilename  The filename for the response to be downloaded as. If null indicates response should
     * not be downloaded.
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
            @QueryParam("asyncAfter") String asyncAfter,
            @QueryParam("filename") String downloadFilename,
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
                asyncAfter,
                downloadFilename,
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
     * @param asyncAfter  The maximum time length (in milliseconds) a request is allowed to be synchronous before
     * becoming asynchronous, if "never" then the request is forever synchronous
     * @param downloadFilename  The filename for the response to be downloaded as. If null indicates response should
     * not be downloaded.
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
            @QueryParam("asyncAfter") String asyncAfter,
            @QueryParam("filename") String downloadFilename,
            @DefaultValue("") @NotNull @QueryParam("perPage") String perPage,
            @DefaultValue("") @NotNull @QueryParam("page") String page,
            @DefaultValue("true") @NotNull @QueryParam("_cache") Boolean readCache,
            @Suspended final AsyncResponse asyncResponse
    ) {
        DataApiRequest apiRequest = null;
        try {
            try (TimedPhase timer = RequestLog.startTiming("DataApiRequest")) {
                apiRequest = dataApiRequestFactory.buildApiRequest(
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
                        formatResolver.apply(format, containerRequestContext),
                        downloadFilename,
                        timeZone,
                        asyncAfter,
                        perPage,
                        page,
                        this
                );
            }

            if (requestMapper != null) {
                try (TimedPhase timer = RequestLog.startTiming("DataApiRequestMappers")) {
                    apiRequest = (DataApiRequest) requestMapper.apply(apiRequest, containerRequestContext);
                }
            }

            // Build the query template
            TemplateDruidQuery templateQuery;
            try (TimedPhase timer = RequestLog.startTiming("DruidQueryMerge")) {
                templateQuery = templateDruidQueryMerger.merge(apiRequest);
            }

            // Select the performance slice and build the final query
            DruidAggregationQuery<?> druidQuery;
            try (TimedPhase timer = RequestLog.startTiming("DruidQueryBuilder")) {
                druidQuery = druidQueryBuilder.buildQuery(apiRequest, templateQuery);
            }

            // Accumulate data needed for request processing workflow
            RequestContext context;
            try (TimedPhase timer = RequestLog.startTiming("BuildRequestContext")) {
                // In embedded contexts, containerRequestContext may be null
                context = new RequestContext(containerRequestContext, readCache);
            }

            Subject<PreResponse, PreResponse> queryResultsEmitter = PublishSubject.create();

            setupAsynchronousWorkflows(
                    apiRequest,
                    queryResultsEmitter,
                    containerRequestContext,
                    asyncResponse,
                    httpResponseMaker
            );

            ResponseProcessor responseProcessor = responseProcessorFactory.build(
                    apiRequest,
                    queryResultsEmitter,
                    druidResponseParser,
                    objectMappers,
                    httpResponseMaker
            );

            try (TimedPhase timer = RequestLog.startTiming("logRequestMetrics")) {
                logRequestMetrics(apiRequest, readCache, druidQuery, templateQuery);
                RequestLog.record(new DruidFilterInfo(druidQuery.getFilter()));
            }

            // Process the request
            RequestLog.startTiming(REQUEST_WORKFLOW_TIMER);
            boolean complete = dataRequestHandler.handleRequest(context, apiRequest, druidQuery, responseProcessor);
            if (!complete) {
                throw new IllegalStateException("No request handler accepted request.");
            }
        } catch (Throwable e) {
            exceptionHandler.handleThrowable(
                e,
                asyncResponse,
                Optional.ofNullable(apiRequest),
                containerRequestContext,
                writer
            );
            // Generally, it's expected that implementations of `ExceptionHandler` will resume
            // the response in every case. This exists so that if someone writes a buggy handler
            // that fails to resume the response, they at least get *something* back.
            if (!asyncResponse.isDone()) {
                asyncResponse.resume(
                    RequestHandlerUtils.makeErrorResponse(
                        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                        e.getClass().getName(),
                        "Failed to handle the following error.",
                        null,
                        writer
                    )
                );
            }
        }
    }

    /**
     * Builds the asynchronous workflows, and subscribes the appropriate channels to the appropriate workflows.
     *
     * @param apiRequest  DataApiRequest object with all the associated info in it
     * @param queryResultsEmitter  The observable that will eventually emit the results of the query
     * @param containerRequestContext  The context for the request
     * @param asyncResponse  The channel over which user responses will be sent
     * @param  httpResponseMaker  The factory for building HTTP responses
     */
    private void setupAsynchronousWorkflows(
            ApiRequest apiRequest,
            Observable<PreResponse> queryResultsEmitter,
            ContainerRequestContext containerRequestContext,
            AsyncResponse asyncResponse,
            HttpResponseMaker httpResponseMaker
    ) {
        UriInfo uriInfo = containerRequestContext.getUriInfo();
        long asyncAfter = apiRequest.getAsyncAfter();
        JobRow jobMetadata = jobRowBuilder.buildJobRow(uriInfo, containerRequestContext);

        // We need to decide when a query is synchronous, and when it should stop being synchronous and become
        // asynchronous. We handle that via the timeout mechanism: If more than asyncAfter milliseconds pass without
        // the queryResultsEmitter emitting a PreResponse (i.e. the system hasn't finished processing the query), then
        // instead of emitting a PreResponse, we emit a JobRow, which triggers all of the asynchronous processing.
        // Also, we will be interacting with external resources, and don't want to interact with those
        // resources once per subscription. A connectable Observable's chain is only executed once
        // regardless of the number of subscriptions.
        ConnectableObservable<Either<PreResponse, JobRow>> payloadEmitter;
        if (asyncAfter == DataApiRequest.ASYNCHRONOUS_ASYNC_AFTER_VALUE) {
            payloadEmitter = Observable.just(Either.<PreResponse, JobRow>right(jobMetadata)).publish();
        } else if (asyncAfter == DataApiRequest.SYNCHRONOUS_ASYNC_AFTER_VALUE) {
            payloadEmitter = queryResultsEmitter.map(Either::<PreResponse, JobRow>left).publish();
        } else {
            payloadEmitter = queryResultsEmitter
                    .map(Either::<PreResponse, JobRow>left)
                    .timeout(
                            asyncAfter,
                            TimeUnit.MILLISECONDS,
                            Observable.fromCallable(() -> Either.right(jobMetadata))
                    )
                    .publish();
        }

        AsynchronousWorkflows asynchronousWorkflows = asynchronousWorkflowsBuilder.buildAsynchronousWorkflows(
                queryResultsEmitter,
                payloadEmitter,
                jobMetadata,
                jobRow -> serializeJobRow(jobRow, uriInfo)
        );

        // Here, we subscribe the HttpResponseChannel to the workflow that generates the Response containing the query
        // results. This workflow will only generate a value if the results are ready within the asyncAfter timeout.
        asynchronousWorkflows.getSynchronousPayload().subscribe(
                new HttpResponseChannel(
                        asyncResponse,
                        apiRequest,
                        containerRequestContext,
                        httpResponseMaker
                )
        );

        // This handles sending the job metadata back to the user in the case where the query becomes asynchronous.
        asynchronousWorkflows.getAsynchronousPayload().subscribe(
                new MetadataHttpResponseChannel(asyncResponse, writer)
        );

        // We don't need to do anything interesting with the notification that the JobRow has been updated, we
        // just need to make sure it gets updated, even if the notifications observable is cold.
        asynchronousWorkflows.getJobMarkedCompleteNotifications().subscribe(
                metadata -> LOG.trace("{} has been updated with a completion status.", metadata),
                error -> LOG.warn("Received an error instead of a notifications that a job was updated.", error)
        );

        // We need to notify other Bard processes that the PreResponse has been stored, so that any long pollers know
        // to go get the results
        asynchronousWorkflows.getPreResponseReadyNotifications().subscribe(
                preResponseStoredNotifications::publish,
                error -> LOG.warn("Received an error instead of a notification that a PreResponse is ready.", error)
        );

        // Now that the workflow has been wired together, we allow the head of the flow to begin emitting items.
        payloadEmitter.connect();
    }

    /**
     * Given a JobRow, and the URI of the request, serializes the JobRow into the version to be sent to the user.
     *
     * @param jobRow  The row to be serialized
     * @param uriInfo  The URI of the request
     *
     * @return A String that should be sent back to the user describing the asynchronous job
     */
    private String serializeJobRow(JobRow jobRow, UriInfo uriInfo) {
        try {
            return objectMappers.getMapper().writeValueAsString(
                    jobPayloadBuilder.buildPayload(jobRow, uriInfo)
            );
        } catch (JsonProcessingException e) {
            LOG.error("Error serializing JobRow: %s", e);
            throw new RuntimeException(e);
        }
    }

    public DruidResponseParser getDruidResponseParser() {
        return druidResponseParser;
    }

    public DruidFilterBuilder getFilterBuilder() {
        return filterBuilder;
    }

    public HavingGenerator getHavingApiGenerator() {
        return havingGenerator;
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

    @Override
    public DateTimeZone getSystemTimeZone() {
        return systemTimeZone;
    }

    @Override
    public DateTimeFormatter getDateTimeFormatter() {
        return dateTimeFormatter;
    }

    @Override
    public ResourceDictionaries getResourceDictionaries() {
        return resourceDictionaries;
    }

    @Override
    public GranularityParser getGranularityParser() {
        return granularityParser;
    }

    @Override
    public ApiRequestLogicalMetricBinder getMetricBinder() {
        return metricBinder;
    }

    @Override
    public LegacyGenerator<List<OrderByColumn>> getOrderByGenerator() {
        return orderByGenerator;
    }
}
