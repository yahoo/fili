// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import static com.yahoo.bard.webservice.config.BardFeatureFlag.UPDATED_METADATA_COLLECTION_NAMES;

import static java.util.AbstractMap.SimpleImmutableEntry;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.OK;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.application.metadataViews.MetadataViewProvider;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.filterbuilders.DruidFilterBuilder;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.TableRequest;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.resolver.QueryPlanningConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.util.TableUtils;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.ResponseFormatResolver;
import com.yahoo.bard.webservice.web.TableFullViewProcessor;
import com.yahoo.bard.webservice.web.TableView;
import com.yahoo.bard.webservice.web.TablesApiRequest;
import com.yahoo.bard.webservice.web.apirequest.HavingGenerator;
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequestImpl;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
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
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Resource code for table resource endpoints.
 */
@Path("/tables")
@Singleton
public class TablesServlet extends EndpointServlet implements BardConfigResources {
    private static final Logger LOG = LoggerFactory.getLogger(TablesServlet.class);

    private final ResourceDictionaries resourceDictionaries;
    private final RequestMapper requestMapper;
    private final GranularityParser granularityParser;
    private final ResponseFormatResolver formatResolver;
    private final Map<String, MetadataViewProvider<?>> metadataBuilders;

    /**
     * Constructor.
     *
     * @param resourceDictionaries  Dictionary holder
     * @param requestMapper  Mapper to change the API request if needed
     * @param objectMappers  JSON tools
     * @param granularityParser  Helper for parsing granularities
     * @param formatResolver  The formatResolver for determining correct response format
     */
    @Inject
    public TablesServlet(
            ResourceDictionaries resourceDictionaries,
            @Named(TablesApiRequest.REQUEST_MAPPER_NAMESPACE) RequestMapper requestMapper,
            @Named(AbstractBinderFactory.METADATA_VIEW_PROVIDERS) Map<String, MetadataViewProvider<?>> metadataBuilders,
            ObjectMappersSuite objectMappers,
            GranularityParser granularityParser,
            ResponseFormatResolver formatResolver
    ) {
        super(objectMappers);
        this.resourceDictionaries = resourceDictionaries;
        this.requestMapper = requestMapper;
        this.metadataBuilders = metadataBuilders;
        this.granularityParser = granularityParser;
        this.formatResolver = formatResolver;
    }

    /**
     * Get all the logical tables as a summary list.
     *
     * @param perPage  number of values to return per page
     * @param page  the page to start from
     * @param format  The name of the output format type
     * @param uriInfo  UriInfo of the request
     * @param containerRequestContext  The context of data provided by the Jersey container for this request
     *
     * @return The list of logical tables
     * <p>
     * {@code
     * {
     *     "tables": <List of Table Summaries>
     * }
     * }
     * @see TablesServlet#getLogicalTableListSummaryView(Collection, UriInfo)
     */
    @GET
    @Timed
    public Response getAllTables(
            @DefaultValue("") @NotNull @QueryParam("perPage") String perPage,
            @DefaultValue("") @NotNull @QueryParam("page") String page,
            @QueryParam("format") String format,
            @Context UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        if (format != null && format.toLowerCase(Locale.ENGLISH).equals("fullview")) {
            return getTablesFullView(perPage, page, uriInfo, containerRequestContext);
        } else {
            return getTable(null, perPage, page, format, uriInfo, containerRequestContext);
        }
    }


    /**
     * Get all grain-specific logical tables for a logical table name as a summary list.
     *
     * @param tableName  Table to get all the grain-specific logical tables for
     * @param perPage  number of values to return per page
     * @param page  the page to start from
     * @param format  The name of the output format type
     * @param uriInfo  UriInfo of the request
     * @param containerRequestContext  The context of data provided by the Jersey container for this request
     *
     * @return The list of grain-specific logical tables
     * <p>
     * {@code
     * {
     *     "tables": <List of Table Summaries>
     * }
     * }
     * @see TablesServlet#getLogicalTableListSummaryView(Collection, UriInfo)
     */
    @GET
    @Timed
    @Path("/{tableName}")
    public Response getTable(
            @PathParam("tableName") String tableName,
            @DefaultValue("") @NotNull @QueryParam("perPage") String perPage,
            @DefaultValue("") @NotNull @QueryParam("page") String page,
            @QueryParam("format") String format,
            @Context UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        Supplier<Response> responseSender;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new TableRequest(tableName != null ? tableName : "all", "all"));

            TablesApiRequestImpl tablesApiRequestImpl = new TablesApiRequestImpl(
                    tableName,
                    null,
                    formatResolver.apply(format, containerRequestContext),
                    perPage,
                    page,
                    uriInfo,
                    this
            );

            if (requestMapper != null) {
                tablesApiRequestImpl = (TablesApiRequestImpl) requestMapper.apply(
                        tablesApiRequestImpl,
                        containerRequestContext
                );
            }

            Stream<Map<String, String>> result = tablesApiRequestImpl.getPage(
                    getLogicalTableListSummaryView(tablesApiRequestImpl.getTables(), containerRequestContext, (MetadataViewProvider<LogicalTable>) metadataBuilders.get("tables.summary.view"))
            );

            Response response = formatResponse(
                    tablesApiRequestImpl,
                    result,
                    UPDATED_METADATA_COLLECTION_NAMES.isOn() ? "tables" : "rows",
                    null
            );
            LOG.debug("Tables Endpoint Response: {}", response.getEntity());
            responseSender = () -> response;
        } catch (RequestValidationException e) {
            LOG.debug(e.getMessage(), e);
            responseSender = () -> Response.status(e.getStatus()).entity(e.getErrorHttpMsg()).build();
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
     * Get <b>unconstrained</b> logical table details for a grain-specific logical table.
     * <p>
     * See {@link
     * #getTableByGrainAndConstraint(String, String, List, String, String, String, UriInfo, ContainerRequestContext)}
     * for getting <b>constrained</b> logical table details
     *
     * @param tableName  Logical table name (part of the logical table ID)
     * @param grain  Logical table grain (part of the logical table ID)
     * @param uriInfo  UriInfo of the request
     * @param containerRequestContext  The context of data provided by the Jersey container for this request
     *
     * @return The grain-specific logical table
     * @see TablesServlet#getLogicalTableFullView(LogicalTable, UriInfo)
     *
     * TODO: Need to delegate to constrained endpoint
     */
    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tableName}/{granularity}")
    public Response getTableByGrain(
            @PathParam("tableName") String tableName,
            @PathParam("granularity") String grain,
            @Context UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        Supplier<Response> responseSender;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new TableRequest(tableName, grain));

            TablesApiRequestImpl tablesApiRequestImpl = new TablesApiRequestImpl(
                    tableName,
                    grain,
                    null,
                    "",
                    "",
                    uriInfo,
                    this
            );

            if (requestMapper != null) {
                tablesApiRequestImpl = (TablesApiRequestImpl) requestMapper.apply(
                        tablesApiRequestImpl,
                        containerRequestContext
                );
            }

            Map<String, Object> result = getLogicalTableFullView(tablesApiRequestImpl.getTable(), containerRequestContext);
            String output = objectMappers.getMapper().writeValueAsString(result);
            LOG.debug("Tables Endpoint Response: {}", output);
            responseSender = () ->  Response.status(Response.Status.OK).entity(output).build();
        } catch (RequestValidationException e) {
            LOG.debug(e.getMessage(), e);
            responseSender = () ->   Response.status(e.getStatus()).entity(e.getErrorHttpMsg()).build();
        } catch (JsonProcessingException e) {
            String msg = ErrorMessageFormat.INTERNAL_SERVER_ERROR_ON_JSON_PROCESSING.format(e.getMessage());
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
     * Get <b>constrained</b> logical table details for a grain-specific logical table.
     * <p>
     * An example query to this endpoint is
     * <pre>
     *     {@code
     *     /tables/myTable/week/dim1/dim2?metrics=myMetric&filters=dim3|id-in[foo,bar]
     *     }
     * </pre>
     * TODO: filter physical tables by interval and filter clause, i.e. "filters=dim3|id-in[foo,bar]".
     * This query has an optional list of path separated grouping dimensions, an optional list of metrics, and an
     * optional filter clause.
     * <p>
     * The query would result in a table response with the metrics, dimensions, and available intervals restricted down
     * to the set of items that are still "reachable" given the constraints in the query (dim1, dim2, dim3, and
     * myMetric, in this case). So, if the table normally indicates that dim7 is one of it's dimensions, but there isn't
     * a backing physical table for myTable that has dim1, dim2, dim3, and myMetric along with dim7, then dim7 would not
     * be in the dimension list returned in the response.
     *
     * @param tableName  Logical table name
     * @param granularity  Logical table grain (part of the logical table ID)
     * @param dimensions  Requested list of dimensions (e.g. dim1, dim2)
     * @param metrics  Requested list of metrics (e.g. myMetric)
     * @param intervals  Requested list of intervals. This is a required
     * @param filters  Requested list of filters (e.g. dim3|id-in[foo,bar])
     * @param uriInfo  UriInfo of the request
     * @param containerRequestContext  The context of data provided by the Jersey container for this request
     *
     * @return The grain-specific logical table
     *
     * See {@link #getLogicalTableFullView(TablesApiRequestImpl, UriInfo)}
     */
    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tableName}/{granularity}/{dimensions:.*}")
    public Response getTableByGrainAndConstraint(
            @PathParam("tableName") String tableName,
            @PathParam("granularity") String granularity,
            @PathParam("dimensions") List<PathSegment> dimensions,
            @QueryParam("metrics") String metrics,
            @QueryParam("dateTime") String intervals,
            @QueryParam("filters") String filters,
            @Context UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        Supplier<Response> responseSender;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new TableRequest(tableName, granularity));

            TablesApiRequestImpl tablesApiRequest = new TablesApiRequestImpl(
                    tableName,
                    granularity,
                    null,
                    "",
                    "",
                    uriInfo,
                    this,
                    dimensions,
                    metrics,
                    intervals,
                    filters,
                    null
            );

            if (requestMapper != null) {
                tablesApiRequest = (TablesApiRequestImpl) requestMapper.apply(
                        tablesApiRequest,
                        containerRequestContext
                );
            }

            Map<String, Object> result = getLogicalTableFullView(tablesApiRequest, containerRequestContext);
            String output = objectMappers.getMapper().writeValueAsString(result);
            LOG.debug("Tables Endpoint Response: {}", output);
            responseSender = () ->  Response.status(OK).entity(output).build();
        } catch (RequestValidationException exception) {
            LOG.debug(exception.getMessage(), exception);
            responseSender = () ->   Response.status(exception.getStatus()).entity(exception.getErrorHttpMsg()).build();
        } catch (JsonProcessingException exception) {
            String message = ErrorMessageFormat.INTERNAL_SERVER_ERROR_ON_JSON_PROCESSING.format(exception.getMessage());
            LOG.error(message, exception);
            responseSender = () -> Response.status(INTERNAL_SERVER_ERROR).entity(message).build();
        } catch (Error | Exception exception) {
            String message = ErrorMessageFormat.REQUEST_PROCESSING_EXCEPTION.format(exception.getMessage());
            LOG.info(message, exception);
            responseSender = () -> Response.status(BAD_REQUEST).entity(message).build();
        } finally {
            RequestLog.stopTiming(this);
        }

        return responseSender.get();
    }

    /**
     * Get all the tables full view.
     *
     * @param perPage  number of values to return per page
     * @param page  the page to start from
     * @param uriInfo  UriInfo of the request
     * @param containerRequestContext  The context of data provided by the Jersey container for this request
     *
     * @return full view of the tables
     */
    @Timed
    public Response getTablesFullView(
            @DefaultValue("") @NotNull @QueryParam("perPage") String perPage,
            @DefaultValue("") @NotNull @QueryParam("page") String page,
            @Context UriInfo uriInfo,
            @Context final ContainerRequestContext containerRequestContext
    ) {
        Supplier<Response> responseSender;
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new TableRequest("all", "all"));

            TablesApiRequestImpl tablesApiRequestImpl = new TablesApiRequestImpl(
                    null,
                    null,
                    null,
                    perPage,
                    page,
                    uriInfo,
                    this
            );

            if (requestMapper != null) {
                tablesApiRequestImpl = (TablesApiRequestImpl) requestMapper.apply(
                        tablesApiRequestImpl,
                        containerRequestContext
                );
            }

            TableFullViewProcessor fullViewProcessor = new TableFullViewProcessor();

            Stream<TableView> paginatedResult = tablesApiRequestImpl.getPage(
                    fullViewProcessor.formatTables(tablesApiRequestImpl.getTables(), containerRequestContext, metadataBuilders)
            );
            Response response = formatResponse(tablesApiRequestImpl, paginatedResult, "tables", null);

            LOG.debug("Tables Endpoint Response: {}", response.getEntity());
            responseSender = () -> response;
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
     * Get the summary list view of the logical tables.
     *
     * @param logicalTables  Collection of logical tables to get the summary view for
     * @param uriInfo  UriInfo of the request
     *
     * @return Summary list view of the logical tables
     */
    public static Set<Map<String, String>> getLogicalTableListSummaryView(
            Collection<LogicalTable> logicalTables,
            ContainerRequestContext containerRequestContext,
            MetadataViewProvider<LogicalTable> metadataViewProvider
    ) {
        return logicalTables.stream()
                .map(logicalTable -> getLogicalTableSummaryView(logicalTable, containerRequestContext, metadataViewProvider))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Get the summary list view of the logical tables.
     *
     * @param logicalTables  Collection of logical tables to get the summary view for
     * @param uriInfo  UriInfo of the request
     *
     * @return Summary list view of the logical tables
     */
    public static Set<Map<String, Object>> getLogicalAll(
            Collection<LogicalTable> logicalTables,
            ContainerRequestContext containerRequestContext
    ) {
        return logicalTables.stream()
                .map(logicalTable -> getLogicalTableFullView(logicalTable, containerRequestContext))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Get the summary view of the logical table.
     *
     * @param logicalTable  Logical table to get the view of
     * @param uriInfo  UriInfo of the request
     *
     * @return Summary view of the logical table
     */
    public static Map<String, String> getLogicalTableSummaryView(
            LogicalTable logicalTable,
            ContainerRequestContext containerRequestContext,
            MetadataViewProvider<LogicalTable> metadataViewProvider
    ) {
        return (Map<String, String>) metadataViewProvider.apply(containerRequestContext, logicalTable);
    }

    /**
     * Get the full view of the logical table.
     *
     * @param logicalTable  Logical table to get the view of
     * @param uriInfo  UriInfo of the request
     *
     * @return Full view of the logical table
     *
     * @deprecated In order to display constrained data availability in table resource, this method needs to accept a
     * {@link com.yahoo.bard.webservice.web.TablesApiRequest} as a parameter. Use
     * {@link #getLogicalTableFullView(TablesApiRequestImpl, UriInfo)} instead.
     */
    @Deprecated
    protected static Map<String, Object> getLogicalTableFullView(LogicalTable logicalTable, ContainerRequestContext containerRequestContext) {
        Map<String, Object> resultRow = new LinkedHashMap<>();
        resultRow.put("category", logicalTable.getCategory());
        resultRow.put("name", logicalTable.getName());
        resultRow.put("longName", logicalTable.getLongName());
        resultRow.put("retention", logicalTable.getRetention().toString());
        resultRow.put("granularity", logicalTable.getGranularity().getName());
        resultRow.put("description", logicalTable.getDescription());
        resultRow.put(
                "dimensions",
                DimensionsServlet.getDimensionListSummaryView(logicalTable.getDimensions(), containerRequestContext.getUriInfo())
        );
        resultRow.put(
                "metrics",
                null
//                MetricsServlet.getLogicalMetricListSummaryView(logicalTable.getLogicalMetrics(), containerRequestContext)
        );
        resultRow.put(
                "availableIntervals",
                logicalTable.getTableGroup().getPhysicalTables().stream()
                        .map(PhysicalTable::getAllAvailableIntervals)
                        .map(Map::entrySet)
                        .flatMap(Set::stream)
                        .map(Map.Entry::getValue)
                        .reduce(new SimplifiedIntervalList(), SimplifiedIntervalList::union)
        );
        return resultRow;
    }

    /**
     * Get the full view of the logical table.
     *
     * @param tablesApiRequest  Table API request that contains information about requested logical table and provides
     * components for constructing a {@link com.yahoo.bard.webservice.table.resolver.QueryPlanningConstraint}, which
     * will be used to filter and constrain table availabilities
     * @param uriInfo  UriInfo of the request
     *
     * @return Full view of the logical table
     */
    protected static Map<String, Object> getLogicalTableFullView(
            TablesApiRequestImpl tablesApiRequest,
            ContainerRequestContext containerRequestContext
    ) {
//        LogicalTable logicalTable = tablesApiRequest.getTable();
//        return Stream.of(
//                new SimpleImmutableEntry<>("category", logicalTable.getCategory()),
//                new SimpleImmutableEntry<>("name", logicalTable.getName()),
//                new SimpleImmutableEntry<>("longName", logicalTable.getLongName()),
//                new SimpleImmutableEntry<>("retention", logicalTable.getRetention().toString()),
//                new SimpleImmutableEntry<>("granularity", logicalTable.getGranularity().getName()),
//                new SimpleImmutableEntry<>("description", logicalTable.getDescription()),
//                new SimpleImmutableEntry<>(
//                        "dimensions",
//                        DimensionsServlet.getDimensionListSummaryView(logicalTable.getDimensions(), containerRequestContext.getUriInfo())
//                ),
//                new SimpleImmutableEntry<>(
//                        "metrics",
//                        MetricsServlet.getLogicalMetricListSummaryView(
//                                logicalTable.getLogicalMetrics(),
//                                containerRequestContext,
//
//                        )
//                ),
//                new SimpleImmutableEntry<>(
//                        "availableIntervals",
//                        TableUtils.getConstrainedLogicalTableAvailability(
//                                logicalTable,
//                                new QueryPlanningConstraint(
//                                        tablesApiRequest.getDimensions(),
//                                        tablesApiRequest.getFilterDimensions(),
//                                        Collections.emptySet(),
//                                        Collections.emptySet(),
//                                        tablesApiRequest.getApiFilters(),
//                                        tablesApiRequest.getTable(),
//                                        Collections.unmodifiableSet(tablesApiRequest.getIntervals()),
//                                        Collections.unmodifiableSet(tablesApiRequest.getLogicalMetrics()),
//                                        tablesApiRequest.getGranularity(),
//                                        tablesApiRequest.getGranularity()
//                                )
//                        )
//                )
//        ).collect(
//                Collectors.toMap(
//                        SimpleImmutableEntry::getKey,
//                        SimpleImmutableEntry::getValue,
//                        (value1OfSameKey, value2OfSameKey) -> value1OfSameKey,
//                        LinkedHashMap::new
//                )
//        );
        return null;
    }

    /**
     * Get the URL of the logical table.
     *
     * @param logicalTable  Logical table to get the URL of
     * @param uriInfo  URI Info for the request
     *
     * @return The absolute URL for the logical table
     */
    public static String getLogicalTableUrl(LogicalTable logicalTable, UriInfo uriInfo) {
        return uriInfo.getBaseUriBuilder()
                .path(TablesServlet.class)
                .path(TablesServlet.class, "getTableByGrain")
                .build(logicalTable.getName(), logicalTable.getGranularity().getName())
                .toASCIIString();
    }

    @Override
    public ResourceDictionaries getResourceDictionaries() {
        return resourceDictionaries;
    }

    public GranularityParser getGranularityParser() {
        return granularityParser;
    }

    /**
     * Filter builder isn't used in TablesServlet but is part of the configuration interface, so this is an empty
     * implementation.
     *
     * @return null because TablesApiRequest doesn't require it
     */
    @Override
    public DruidFilterBuilder getFilterBuilder() {
        return null;
    }

    /**
     * Having Api generator isn't used in TablesServlet but is part of the configuration interface, so this is an empty
     * implementation.
     *
     * @return null because TablesApiRequest doesn't require it
     */
    @Override
    public HavingGenerator getHavingApiGenerator() {
        return null;
    }


    /**
     * SystemTimeZone isn't used in TablesServlet but is part of the configuration interface, so this is an empty
     * implementation.
     *
     * @return null because TablesApiRequest doesn't require it
     */
    @Override
    public DateTimeZone getSystemTimeZone() {
        return null;
    }

    public RequestMapper getRequestMapper() {
        return requestMapper;
    }

    public LogicalTableDictionary getLogicalTableDictionary() {
        return resourceDictionaries.getLogicalDictionary();
    }
}
