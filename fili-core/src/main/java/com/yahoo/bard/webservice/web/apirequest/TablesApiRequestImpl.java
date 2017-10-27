// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.EMPTY_DICTIONARY;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_GRANULARITY_MISMATCH;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_UNDEFINED;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.ForTesting;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.TablesApiRequest;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import com.yahoo.bard.webservice.web.util.PaginationParameters;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Tables API Request Implementation binds, validates, and models the parts of a request to the table endpoint.
 */
public class TablesApiRequestImpl extends ApiRequestImpl implements TablesApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(TablesApiRequestImpl.class);

    private final Set<LogicalTable> tables;
    private final LogicalTable table;
    private final Granularity granularity;
    private final Set<Dimension> dimensions;
    private final Set<LogicalMetric> metrics;
    private final Set<Interval> intervals;
    private final Map<Dimension, Set<ApiFilter>> filters;

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  string time granularity in URL
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param uriInfo  The URI of the request object.
     * @param bardConfigResources  The configuration resources used to build this api request
     *
     * @throws BadApiRequestException is thrown in the following scenarios:
     * <ol>
     *     <li>Invalid table in the API request.</li>
     *     <li>Pagination parameters in the API request that are not positive integers.</li>
     * </ol>
     */
    public TablesApiRequestImpl(
            String tableName,
            String granularity,
            String format,
            @NotNull String perPage,
            @NotNull String page,
            UriInfo uriInfo,
            BardConfigResources bardConfigResources
    ) throws BadApiRequestException {
        super(format, perPage, page, uriInfo);

        this.tables = generateTables(tableName, bardConfigResources.getLogicalTableDictionary());

        if (tableName != null && granularity != null) {
            this.granularity = generateGranularity(granularity, bardConfigResources.getGranularityParser());
            this.table = generateTable(tableName, this.granularity, bardConfigResources.getLogicalTableDictionary());
        } else {
            this.table = null;
            this.granularity = null;
        }

        dimensions = Collections.emptySet();
        metrics = Collections.emptySet();
        intervals = Collections.emptySet();
        filters = Collections.emptyMap();

        LOG.debug(
                "Api request: Tables: {},\nGranularity: {},\nFormat: {}\nPagination: {}" +
                        "\nDimensions: {}\nMetrics: {}\nIntervals: {}\nFilters: {}",
                this.tables,
                this.granularity,
                this.format,
                this.paginationParameters,
                this.dimensions,
                this.metrics,
                this.intervals,
                this.filters
        );
    }

    /**
     * Parses the API request URL and generates the API Request object with specified query constraints, i.e.
     * dimensions, metrics, date time intervals, and filters.
     *
     * @param tableName  Logical table corresponding to the table name specified in the URL
     * @param granularity  Requested time granularity
     * @param format  Response data format JSON or CSV. Default is JSON.
     * @param perPage  Number of rows to display per page of results. It must represent a positive integer or an empty
     * string if it's not specified
     * @param page  Desired page of results. It must represent a positive integer or an empty
     * string if it's not specified
     * @param uriInfo  The URI of the request
     * @param bardConfigResources  The configuration resources used to build this API request
     * @param dimensions  Grouping dimensions / Dimension constraint
     * @param metrics  Metrics constraint
     * @param intervals  Data / Time constraint
     * @param filters  Filter constraint
     * @param timeZoneId  A joda time zone id
     *
     * @throws BadApiRequestException on
     * <ol>
     *     <li>invalid table</li>
     *     <li>invalid pagination parameters</li>
     * </ol>
     */
    public TablesApiRequestImpl(
            String tableName,
            String granularity,
            String format,
            @NotNull String perPage,
            @NotNull String page,
            UriInfo uriInfo,
            BardConfigResources bardConfigResources,
            List<PathSegment> dimensions,
            String metrics,
            String intervals,
            String filters,
            String timeZoneId
    ) throws BadApiRequestException {
        super(format, perPage, page, uriInfo);

        LogicalTableDictionary logicalTableDictionary = bardConfigResources.getLogicalTableDictionary();
        this.tables = generateTables(tableName, logicalTableDictionary);

        if (tableName != null && granularity != null) {
            this.granularity = generateGranularity(granularity, bardConfigResources.getGranularityParser());
            this.table = generateTable(tableName, this.granularity, logicalTableDictionary);
        } else {
            this.table = null;
            this.granularity = null;
        }

        // parse dimensions
        DimensionDictionary dimensionDictionary = bardConfigResources.getDimensionDictionary();
        this.dimensions = generateDimensions(dimensions, dimensionDictionary);
        validateRequestDimensions(this.dimensions, this.table);

        // parse metrics
        this.metrics = generateLogicalMetrics(
                metrics,
                bardConfigResources.getMetricDictionary().getScope(Collections.singletonList(tableName))
        );
        validateMetrics(this.metrics, this.table);

        // parse interval
        this.intervals = generateIntervals(
                intervals,
                this.granularity,
                generateDateTimeFormatter(
                        generateTimeZone(
                                timeZoneId,
                                bardConfigResources.getSystemTimeZone()
                        )
                )
        );
        validateTimeAlignment(this.granularity, this.intervals);

        // parse filters
        this.filters = generateFilters(filters, table, dimensionDictionary);
        validateRequestDimensions(this.filters.keySet(), this.table);

        LOG.debug(
                "Api request: Tables: {},\nGranularity: {},\nFormat: {}\nPagination: {}" +
                        "\nDimensions: {}\nMetrics: {}\nIntervals: {}\nFilters: {}",
                this.tables,
                this.granularity,
                this.format,
                this.paginationParameters,
                this.dimensions.stream()
                        .map(Dimension::getApiName)
                        .collect(Collectors.toList()),
                this.metrics.stream()
                        .map(LogicalMetric::getName)
                        .collect(Collectors.toList()),
                this.intervals,
                this.filters.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        entry -> entry.getKey().getApiName(),
                                        Function.identity()

                                )
                        )
        );
    }

    /**
     * All argument constructor and its used primarily for withers - hence its private.
     *
     * @param format Response data format JSON or CSV. Default is JSON
     * @param paginationParameters The parameters used to describe pagination
     * @param uriInfo The URI of the request object
     * @param builder The response builder for this request
     * @param tables Set of logical tables
     * @param table Logical table
     * @param granularity Requested time granularity
     * @param dimensions Grouping dimensions / Dimension constraint
     * @param metrics Metrics constraint
     * @param intervals Data / Time constraint
     * @param filters Filter constraint
     */
    private TablesApiRequestImpl(
            ResponseFormatType format,
            Optional<PaginationParameters> paginationParameters,
            UriInfo uriInfo,
            Response.ResponseBuilder builder,
            Set<LogicalTable> tables,
            LogicalTable table,
            Granularity granularity,
            Set<Dimension> dimensions,
            Set<LogicalMetric> metrics,
            Set<Interval> intervals,
            Map<Dimension, Set<ApiFilter>> filters
    ) {
        super(format, SYNCHRONOUS_ASYNC_AFTER_VALUE, paginationParameters, uriInfo, builder);
        this.tables = tables;
        this.table = table;
        this.granularity = granularity;
        this.dimensions = dimensions;
        this.metrics = metrics;
        this.intervals = intervals;
        this.filters = filters;
    }

    /**
     * No argument constructor, meant to be used only for testing.
     *
     * @deprecated it's not a good practice to have testing code here. This constructor will be removed entirely.
     */
    @Deprecated
    @ForTesting
    protected TablesApiRequestImpl() {
        super();
        this.tables = null;
        this.table = null;
        this.granularity = null;
        this.dimensions = null;
        this.metrics = null;
        this.intervals = null;
        this.filters = null;
    }

    /**
     * Extracts the list of logical table names from the url table path segment and generates a set of logical table
     * objects based on it.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param tableDictionary  Logical table dictionary contains the map of valid table names and table objects.
     *
     * @return Set of logical table objects.
     * @throws BadApiRequestException if an invalid table is requested or the logical table dictionary is empty.
     */
    protected Set<LogicalTable> generateTables(String tableName, LogicalTableDictionary tableDictionary)
            throws BadApiRequestException {
        Set<LogicalTable> generated = tableDictionary.values().stream()
                .filter(logicalTable -> tableName == null || tableName.equals(logicalTable.getName()))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // Check if logical tables exist with the requested logical table name
        if (generated.isEmpty()) {
            String msg;
            if (tableDictionary.isEmpty()) {
                msg = EMPTY_DICTIONARY.logFormat("Logical Table");
            } else {
                msg = TABLE_UNDEFINED.logFormat(tableName);
            }
            LOG.error(msg);
            throw new BadApiRequestException(msg);
        }

        LOG.trace("Generated set of logical tables: {}", generated);
        return generated;
    }

    /**
     * Extracts a specific logical table object given a valid table name and a valid granularity.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  logical table corresponding to the table name specified in the URL
     * @param tableDictionary  Logical table dictionary contains the map of valid table names and table objects.
     *
     * @return Set of logical table objects.
     * @throws BadApiRequestException Invalid table exception if the table dictionary returns a null.
     */
    protected LogicalTable generateTable(
            String tableName,
            Granularity granularity,
            LogicalTableDictionary tableDictionary
    ) throws BadApiRequestException {
        LogicalTable generated = tableDictionary.get(new TableIdentifier(tableName, granularity));

        // check if requested logical table grain pair exists
        if (generated == null) {
            String msg = TABLE_GRANULARITY_MISMATCH.logFormat(granularity, tableName);
            LOG.error(msg);
            throw new BadApiRequestException(msg);
        }

        LOG.trace("Generated logical table: {} with granularity {}", generated, granularity);
        return generated;
    }

    /**
     * Extracts the list of metrics from the url metric query string and generates a set of LogicalMetrics.
     *
     * @param apiMetricQuery  URL query string containing the metrics separated by ','.
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects.
     *
     * @return Set of metric objects.
     * @throws BadApiRequestException if the metric dictionary returns a null or if the apiMetricQuery is invalid.
     */
    protected LinkedHashSet<LogicalMetric> generateLogicalMetrics(
            String apiMetricQuery,
            MetricDictionary metricDictionary
    ) throws BadApiRequestException {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingLogicalMetrics")) {
            LOG.trace("Metric dictionary: {}", metricDictionary);

            if (apiMetricQuery == null || "".equals(apiMetricQuery)) {
                return new LinkedHashSet<>();
            }
            // set of logical metric objects
            LinkedHashSet<LogicalMetric> generated = new LinkedHashSet<>();
            List<String> invalidMetricNames = new ArrayList<>();

            List<String> metricApiQuery = Arrays.asList(apiMetricQuery.split(","));
            for (String metricName : metricApiQuery) {
                LogicalMetric logicalMetric = metricDictionary.get(metricName);

                // If metric dictionary returns a null, it means the requested metric is not found.
                if (logicalMetric == null) {
                    invalidMetricNames.add(metricName);
                } else {
                    generated.add(logicalMetric);
                }
            }

            if (!invalidMetricNames.isEmpty()) {
                LOG.debug(METRICS_UNDEFINED.logFormat(invalidMetricNames.toString()));
                throw new BadApiRequestException(METRICS_UNDEFINED.format(invalidMetricNames.toString()));
            }
            LOG.trace("Generated set of logical metric: {}", generated);
            return generated;
        }
    }

    @Override
    public Set<LogicalTable> getTables() {
        return this.tables;
    }

    @Override
    public LogicalTable getTable() {
        return this.table;
    }

    @Override
    public Granularity getGranularity() {
        return this.granularity;
    }

    //CHECKSTYLE:OFF
    @Override
    public TablesApiRequest withFormat(ResponseFormatType format) {
        return new TablesApiRequestImpl(format, paginationParameters, uriInfo, builder, tables, table, granularity, dimensions, metrics, intervals, filters);
    }

    @Override
    public TablesApiRequest withPaginationParameters(Optional<PaginationParameters> paginationParameters) {
        return new TablesApiRequestImpl(format, paginationParameters, uriInfo, builder, tables, table, granularity, dimensions, metrics, intervals, filters);
    }

    @Override
    public TablesApiRequest withUriInfo(UriInfo uriInfo) {
        return new TablesApiRequestImpl(format, paginationParameters, uriInfo, builder, tables, table, granularity, dimensions, metrics, intervals, filters);
    }

    @Override
    public TablesApiRequest withBuilder(Response.ResponseBuilder builder) {
        return new TablesApiRequestImpl(format, paginationParameters, uriInfo, builder, tables, table, granularity, dimensions, metrics, intervals, filters);
    }

    @Override
    public TablesApiRequest withTables(Set<LogicalTable> tables) {
        return new TablesApiRequestImpl(format, paginationParameters, uriInfo, builder, tables, table, granularity, dimensions, metrics, intervals, filters);
    }

    public TablesApiRequest withTable(LogicalTable table) {
        return new TablesApiRequestImpl(format, paginationParameters, uriInfo, builder, tables, table, granularity, dimensions, metrics, intervals, filters);
    }

    @Override
    public TablesApiRequest withGranularity(Set<LogicalTable> tables) {
        return new TablesApiRequestImpl(format, paginationParameters, uriInfo, builder, tables, table, granularity, dimensions, metrics, intervals, filters);
    }

    @Override
    public TablesApiRequest withTables(Granularity granularity) {
        return new TablesApiRequestImpl(format, paginationParameters, uriInfo, builder, tables, table, granularity, dimensions, metrics, intervals, filters);
    }

    @Override
    public TablesApiRequest withDimensions(Set<Dimension> dimensions) {
        return new TablesApiRequestImpl(format, paginationParameters, uriInfo, builder, tables, table, granularity, dimensions, metrics, intervals, filters);
    }

    @Override
    public TablesApiRequest withMetrics(Set<LogicalMetric> metrics) {
        return new TablesApiRequestImpl(format, paginationParameters, uriInfo, builder, tables, table, granularity, dimensions, metrics, intervals, filters);
    }

    @Override
    public TablesApiRequest withIntervals(Set<Interval> intervals) {
        return new TablesApiRequestImpl(format, paginationParameters, uriInfo, builder, tables, table, granularity, dimensions, metrics, intervals, filters);
    }

    @Override
    public TablesApiRequest withFilters(Map<Dimension, Set<ApiFilter>> filters) {
        return new TablesApiRequestImpl(format, paginationParameters, uriInfo, builder, tables, table, granularity, dimensions, metrics, intervals, filters);
    }
    //CHECKSTYLE:ON
}
