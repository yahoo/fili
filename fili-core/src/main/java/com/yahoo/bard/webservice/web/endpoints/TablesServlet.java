// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import static com.yahoo.bard.webservice.config.BardFeatureFlag.UPDATED_METADATA_COLLECTION_NAMES;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

import com.yahoo.bard.webservice.application.ObjectMappersSuite;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.filterbuilders.DruidFilterBuilder;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.blocks.TableRequest;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.TableFullViewProcessor;
import com.yahoo.bard.webservice.web.TableView;
import com.yahoo.bard.webservice.web.TablesApiRequest;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

    /**
     * Constructor.
     *
     * @param resourceDictionaries  Dictionary holder
     * @param requestMapper  Mapper to change the API request if needed
     * @param objectMappers  JSON tools
     * @param granularityParser  Helper for parsing granularities
     */
    @Inject
    public TablesServlet(
            ResourceDictionaries resourceDictionaries,
            @Named(TablesApiRequest.REQUEST_MAPPER_NAMESPACE) RequestMapper requestMapper,
            ObjectMappersSuite objectMappers,
            GranularityParser granularityParser
    ) {
        super(objectMappers);
        this.resourceDictionaries = resourceDictionaries;
        this.requestMapper = requestMapper;
        this.granularityParser = granularityParser;
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
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new TableRequest(tableName != null ? tableName : "all", "all"));

            TablesApiRequest apiRequest = new TablesApiRequest(
                    tableName,
                    null,
                    format,
                    perPage,
                    page,
                    uriInfo,
                    this
            );

            if (requestMapper != null) {
                apiRequest = (TablesApiRequest) requestMapper.apply(apiRequest, containerRequestContext);
            }

            Stream<Map<String, String>> result = apiRequest.getPage(
                    getLogicalTableListSummaryView(apiRequest.getTables(), uriInfo)
            );

            Response response = formatResponse(
                    apiRequest,
                    result,
                    UPDATED_METADATA_COLLECTION_NAMES.isOn() ? "tables" : "rows",
                    null
            );
            LOG.debug("Tables Endpoint Response: {}", response.getEntity());
            RequestLog.stopTiming(this);
            return response;
        } catch (RequestValidationException e) {
            LOG.debug(e.getMessage(), e);
            RequestLog.stopTiming(this);
            return Response.status(e.getStatus()).entity(e.getErrorHttpMsg()).build();
        } catch (Error | Exception e) {
            String msg = String.format("Exception processing request: %s", e.getMessage());
            LOG.info(msg, e);
            RequestLog.stopTiming(this);
            return Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
        }
    }

    /**
     * Get the logical table details for a grain-specific logical table.
     *
     * @param tableName  Logical table name (part of the logical table ID)
     * @param grain  Logical table grain (part of the logical table ID)
     * @param uriInfo  UriInfo of the request
     * @param containerRequestContext  The context of data provided by the Jersey container for this request
     *
     * @return The grain-specific logical table
     * @see TablesServlet#getLogicalTableFullView(LogicalTable, UriInfo)
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
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new TableRequest(tableName, grain));

            TablesApiRequest apiRequest = new TablesApiRequest(
                    tableName,
                    grain,
                    null,
                    "",
                    "",
                    uriInfo,
                    this
            );

            if (requestMapper != null) {
                apiRequest = (TablesApiRequest) requestMapper.apply(apiRequest, containerRequestContext);
            }

            Map<String, Object> result = getLogicalTableFullView(apiRequest.getTable(), uriInfo);
            String output = objectMappers.getMapper().writeValueAsString(result);
            LOG.debug("Tables Endpoint Response: {}", output);
            RequestLog.stopTiming(this);
            return Response.status(Response.Status.OK).entity(output).build();
        } catch (RequestValidationException e) {
            LOG.debug(e.getMessage(), e);
            RequestLog.stopTiming(this);
            return Response.status(e.getStatus()).entity(e.getErrorHttpMsg()).build();
        } catch (JsonProcessingException e) {
            String msg = String.format("Internal server error. JsonProcessingException : %s", e.getMessage());
            LOG.error(msg, e);
            RequestLog.stopTiming(this);
            return Response.status(INTERNAL_SERVER_ERROR).entity(msg).build();
        } catch (Error | Exception e) {
            String msg = String.format("Exception processing request: %s", e.getMessage());
            LOG.info(msg, e);
            RequestLog.stopTiming(this);
            return Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
        }
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
        try {
            RequestLog.startTiming(this);
            RequestLog.record(new TableRequest("all", "all"));

            TablesApiRequest tablesApiRequest = new TablesApiRequest(
                    null,
                    null,
                    null,
                    perPage,
                    page,
                    uriInfo,
                    this
            );

            if (requestMapper != null) {
                tablesApiRequest = (TablesApiRequest) requestMapper.apply(tablesApiRequest, containerRequestContext);
            }

            TableFullViewProcessor fullViewProcessor = new TableFullViewProcessor();

            Stream<TableView> paginatedResult = tablesApiRequest.getPage(
                    fullViewProcessor.formatTables(tablesApiRequest.getTables(), uriInfo)
            );
            Response response = formatResponse(tablesApiRequest, paginatedResult, "tables", null);

            LOG.debug("Tables Endpoint Response: {}", response.getEntity());
            RequestLog.stopTiming(this);
            return response;
        } catch (Error | Exception e) {
            String msg = String.format("Exception processing request: %s", e.getMessage());
            LOG.info(msg, e);
            RequestLog.stopTiming(this);
            return Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
        }
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
            UriInfo uriInfo
    ) {
        return logicalTables.stream()
                .map(logicalTable -> getLogicalTableSummaryView(logicalTable, uriInfo))
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
            UriInfo uriInfo
    ) {
        return logicalTables.stream()
                .map(logicalTable -> getLogicalTableFullView(logicalTable, uriInfo))
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
    public static Map<String, String> getLogicalTableSummaryView(LogicalTable logicalTable, UriInfo uriInfo) {
        Map<String, String> resultRow = new LinkedHashMap<>();
        resultRow.put("category", logicalTable.getCategory());
        resultRow.put("name", logicalTable.getName());
        resultRow.put("longName", logicalTable.getLongName());
        resultRow.put("granularity", logicalTable.getGranularity().getName());
        resultRow.put("uri", getLogicalTableUrl(logicalTable, uriInfo));
        return resultRow;
    }

    /**
     * Get the full view of the logical table.
     *
     * @param logicalTable  Logical table to get the view of
     * @param uriInfo  UriInfo of the request
     *
     * @return Full view of the logical table
     */
    protected static Map<String, Object> getLogicalTableFullView(LogicalTable logicalTable, UriInfo uriInfo) {
        Map<String, Object> resultRow = new LinkedHashMap<>();
        resultRow.put("category", logicalTable.getCategory());
        resultRow.put("name", logicalTable.getName());
        resultRow.put("longName", logicalTable.getLongName());
        resultRow.put("retention", logicalTable.getRetention().toString());
        resultRow.put("granularity", logicalTable.getGranularity().getName());
        resultRow.put("description", logicalTable.getDescription());
        resultRow.put(
                "dimensions",
                DimensionsServlet.getDimensionListSummaryView(logicalTable.getDimensions(), uriInfo)
        );
        resultRow.put(
                "metrics",
                MetricsServlet.getLogicalMetricListSummaryView(logicalTable.getLogicalMetrics(), uriInfo)
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
