// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.EMPTY_DICTIONARY;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_UNDEFINED;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.apirequest.binders.FilterBinders;
import com.yahoo.bard.webservice.web.apirequest.binders.FilterGenerator;
import com.yahoo.bard.webservice.web.filters.ApiFilters;
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

/**
 * Tables API Request Implementation binds, validates, and models the parts of a request to the table endpoint.
 */
public class TablesApiRequestImpl extends ApiRequestImpl implements TablesApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(TablesApiRequestImpl.class);

    private final LinkedHashSet<LogicalTable> tables;
    private final LogicalTable table;
    private final Granularity granularity;
    private final List<Interval> intervals;
    private final LinkedHashSet<Dimension> dimensions;
    private final LinkedHashSet<LogicalMetric> logicalMetrics;
    private final ApiFilters apiFilters;

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
            BardConfigResources bardConfigResources
    ) throws BadApiRequestException {
        this(tableName, granularity, format, null, perPage, page, bardConfigResources);
    }

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  string time granularity in URL
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param downloadFilename If not null and not empty, indicates the response should be downloaded by the client with
     * the provided filename. Otherwise indicates the response should be rendered in the browser.
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
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
            String downloadFilename,
            @NotNull String perPage,
            @NotNull String page,
            BardConfigResources bardConfigResources
    ) throws BadApiRequestException {
        super(format, downloadFilename, SYNCHRONOUS_REQUEST_FLAG, perPage, page);

        this.tables = generateTables(tableName, bardConfigResources.getLogicalTableDictionary());

        if (tableName != null && granularity != null) {
            this.granularity = generateGranularity(granularity, bardConfigResources.getGranularityParser());
            this.table = generateTable(tableName, this.granularity, bardConfigResources.getLogicalTableDictionary());
            this.apiFilters = table.getFilters().map(ApiFilters::new).orElse(new ApiFilters());
        } else {
            this.table = null;
            this.granularity = null;
            this.apiFilters = null;
        }

        intervals = Collections.emptyList();
        dimensions = new LinkedHashSet<>();
        logicalMetrics = new LinkedHashSet<>();

        LOG.debug(
                "Api request: Tables: {},\nGranularity: {},\nFormat: {},\nFilename: {},\nPagination: {}" +
                        "\nDimensions: {}\nMetrics: {}\nIntervals: {}\nFilters: {}",
                this.tables,
                this.granularity,
                this.format,
                this.downloadFilename,
                this.paginationParameters,
                this.dimensions,
                this.logicalMetrics,
                this.intervals,
                this.apiFilters
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
     * @deprecated prefer constructor with downloadFilename
     */
    @Deprecated
    public TablesApiRequestImpl(
            String tableName,
            String granularity,
            String format,
            @NotNull String perPage,
            @NotNull String page,
            BardConfigResources bardConfigResources,
            List<PathSegment> dimensions,
            String metrics,
            String intervals,
            String filters,
            String timeZoneId
    ) throws BadApiRequestException {
        this(
                tableName,
                granularity,
                format,
                null,
                perPage,
                page,
                bardConfigResources,
                dimensions,
                metrics,
                intervals,
                filters,
                timeZoneId
        );
    }

    /**
     * Parses the API request URL and generates the API Request object with specified query constraints, i.e.
     * dimensions, metrics, date time intervals, and filters.
     *
     * @param tableName  Logical table corresponding to the table name specified in the URL
     * @param granularity  Requested time granularity
     * @param format  Response data format JSON or CSV. Default is JSON.
     * @param downloadFilename If not null and not empty, indicates the response should be downloaded by the client with
     * the provided filename. Otherwise indicates the response should be rendered in the browser.
     * @param perPage  Number of rows to display per page of results. It must represent a positive integer or an empty
     * string if it's not specified
     * @param page  Desired page of results. It must represent a positive integer or an empty
     * string if it's not specified
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
            String downloadFilename,
            @NotNull String perPage,
            @NotNull String page,
            BardConfigResources bardConfigResources,
            List<PathSegment> dimensions,
            String metrics,
            String intervals,
            String filters,
            String timeZoneId
    ) throws BadApiRequestException {
        super(format, downloadFilename, SYNCHRONOUS_REQUEST_FLAG, perPage, page);

        if (granularity == null || tableName == null) {
            throw new BadApiRequestException("Logical table and granularity cannot be null");
        }

        LogicalTableDictionary logicalTableDictionary = bardConfigResources.getLogicalTableDictionary();
        DimensionDictionary dimensionDictionary = bardConfigResources.getDimensionDictionary();
        this.tables = generateTables(tableName, logicalTableDictionary);

        this.granularity = generateGranularity(granularity, bardConfigResources.getGranularityParser());
        this.table = generateTable(tableName, this.granularity, logicalTableDictionary);

        // parse dimensions
        this.dimensions = generateDimensions(dimensions, dimensionDictionary);
        validateRequestDimensions(this.dimensions, this.table);

        // parse metrics
        this.logicalMetrics = generateLogicalMetrics(
                metrics,
                bardConfigResources.getMetricDictionary().getScope(Collections.singletonList(tableName))
        );
        validateMetrics(this.logicalMetrics, this.table);

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
        ApiFilters requestFilters = getFilterGenerator().generate(filters, table, dimensionDictionary);
        this.apiFilters = table.getFilters()
                .map(f -> ApiFilters.union(f, requestFilters))
                .orElse(requestFilters);

        validateRequestDimensions(getFilterDimensions(), this.table);

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
                this.logicalMetrics.stream()
                        .map(LogicalMetric::getName)
                        .collect(Collectors.toList()),
                this.intervals,
                this.apiFilters.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        entry -> entry.getKey().getApiName(),
                                        Function.identity()

                                )
                        )
        );
    }

    /**
     * Get a filter generator binder for writing ApiFilters.
     *
     * @return An implementation of FilterGenerator.
     */
    protected FilterGenerator getFilterGenerator() {
        return FilterBinders.getInstance()::generateFilters;
    }

    /**
     * All argument constructor and its used primarily for withers - hence its private.
     *
     * @param format Response data format JSON or CSV. Default is JSON
     * @param paginationParameters The parameters used to describe pagination
     * @param tables Set of logical tables
     * @param table Logical table
     * @param granularity Requested time granularity
     * @param dimensions Grouping dimensions / Dimension constraint
     * @param metrics Metrics constraint
     * @param intervals Data / Time constraint
     * @param filters Filter constraint
     * @deprecated prefer constructor with downloadFilename
     */
    @Deprecated
    private TablesApiRequestImpl(
            ResponseFormatType format,
            Optional<PaginationParameters> paginationParameters,
            LinkedHashSet<LogicalTable> tables,
            LogicalTable table,
            Granularity granularity,
            LinkedHashSet<Dimension> dimensions,
            LinkedHashSet<LogicalMetric> metrics,
            List<Interval> intervals,
            ApiFilters filters
    ) {
        this(format, null, paginationParameters, tables, table, granularity, dimensions, metrics, intervals, filters);
    }

    /**
     * All argument constructor and its used primarily for withers - hence its private.
     *
     * @param format Response data format JSON or CSV. Default is JSON
     * @param downloadFilename If not null and not empty, indicates the response should be downloaded by the client with
     * the provided filename. Otherwise indicates the response should be rendered in the browser.
     * @param paginationParameters The parameters used to describe pagination
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
            String downloadFilename,
            Optional<PaginationParameters> paginationParameters, // TODO should this be refactored to not be optional?
            LinkedHashSet<LogicalTable> tables,
            LogicalTable table,
            Granularity granularity,
            LinkedHashSet<Dimension> dimensions,
            LinkedHashSet<LogicalMetric> metrics,
            List<Interval> intervals,
            ApiFilters filters
    ) {
        super(format, downloadFilename, SYNCHRONOUS_ASYNC_AFTER_VALUE, paginationParameters);
        this.tables = tables;
        this.table = table;
        this.granularity = granularity;
        this.dimensions = dimensions;
        this.logicalMetrics = metrics;
        this.intervals = intervals;
        this.apiFilters = table == null ?
                filters
                : table.getFilters().map(f -> ApiFilters.union(f, filters)).orElse(filters);
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
    protected LinkedHashSet<LogicalTable> generateTables(String tableName, LogicalTableDictionary tableDictionary)
            throws BadApiRequestException {
        LinkedHashSet<LogicalTable> generated = tableDictionary.values().stream()
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

    @Override
    public Set<Dimension> getDimensions() {
        return dimensions;
    }

    @Override
    public Set<Dimension> getFilterDimensions() {
        return apiFilters.keySet();
    }

    @Override
    public ApiFilters getApiFilters() {
        return apiFilters;
    }

    @Override
    public List<Interval> getIntervals() {
        return intervals;
    }

    @Override
    public Set<LogicalMetric> getLogicalMetrics() {
        return logicalMetrics;
    }

    //CHECKSTYLE:OFF
    @Override
    public TablesApiRequest withFormat(ResponseFormatType format) {
        return new TablesApiRequestImpl(
                format,
                downloadFilename,
                paginationParameters,
                tables,
                table,
                granularity,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters
        );
    }

    @Override
    public TablesApiRequest withPaginationParameters(Optional<PaginationParameters> paginationParameters) {
        return new TablesApiRequestImpl(
                format,
                downloadFilename,
                paginationParameters,
                tables,
                table,
                granularity,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters
        );
    }

    @Override
    public TablesApiRequest withBuilder(Response.ResponseBuilder builder) {
        return new TablesApiRequestImpl(
                format,
                downloadFilename,
                paginationParameters,
                tables,
                table,
                granularity,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters
        );
    }

    @Override
    public TablesApiRequest withTables(LinkedHashSet<LogicalTable> tables) {
        return new TablesApiRequestImpl(
                format,
                downloadFilename,
                paginationParameters,
                tables,
                table,
                granularity,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters
        );
    }

    public TablesApiRequest withTable(LogicalTable table) {
        return new TablesApiRequestImpl(
                format,
                downloadFilename,
                paginationParameters,
                tables,
                table,
                granularity,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters
        );
    }


    @Override
    public TablesApiRequest withMetrics(LinkedHashSet<LogicalMetric> logicalMetrics) {
        return new TablesApiRequestImpl(
                format,
                downloadFilename,
                paginationParameters,
                tables,
                table,
                granularity,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters
        );
    }

    @Override
    public TablesApiRequest withGranularity(Granularity granularity) {
        return new TablesApiRequestImpl(
                format,
                downloadFilename,
                paginationParameters,
                tables,
                table,
                granularity,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters
        );
    }

    public TablesApiRequest withDimensions(LinkedHashSet<Dimension> dimensions) {
        return new TablesApiRequestImpl(
                format,
                downloadFilename,
                paginationParameters,
                tables,
                table,
                granularity,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters
        );
    }

    @Override
    @Deprecated
    public TablesApiRequest withIntervals(Set<Interval> intervals) {
        return new TablesApiRequestImpl(
                format,
                downloadFilename,
                paginationParameters,
                tables,
                table,
                granularity,
                dimensions,
                logicalMetrics,
                new ArrayList<>(intervals),
                apiFilters
        );
    }

    @Override
    public TablesApiRequest withIntervals(List<Interval> intervals) {
        return new TablesApiRequestImpl(
                format,
                downloadFilename,
                paginationParameters,
                tables,
                table,
                granularity,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters
        );
    }

    @Override
    public TablesApiRequest withDownloadFilename(String downloadFilename) {
        return new TablesApiRequestImpl(
                format,
                downloadFilename,
                paginationParameters,
                tables,
                table,
                granularity,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters
        );
    }

    @Override
    public TablesApiRequest withFilters(Map<Dimension, Set<ApiFilter>> filters) {
        return new TablesApiRequestImpl(
                format,
                downloadFilename,
                paginationParameters,
                tables,
                table,
                granularity,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters
        );
    }
    //CHECKSTYLE:ON
}
