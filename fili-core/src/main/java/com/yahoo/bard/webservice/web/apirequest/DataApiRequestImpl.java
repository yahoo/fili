// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DIMENSION_FIELDS_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INCORRECT_METRIC_FILTER_FORMAT;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTEGER_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_ASYNC_AFTER;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_METRIC_FILTER_CONDITION;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_TIME_ZONE;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_MISSING;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.NON_AGGREGATABLE_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_DIRECTION_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.UNSUPPORTED_FILTERED_METRIC_CATEGORY;
import static com.yahoo.bard.webservice.web.apirequest.ApiRequestImpl.DEFAULT_PAGINATION;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.FilterBuilderException;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.model.builders.DefaultDruidHavingBuilder;
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection;
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.util.AllPagesPagination;
import com.yahoo.bard.webservice.util.StreamUtils;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.ApiHaving;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.DefaultFilterOperation;
import com.yahoo.bard.webservice.web.DimensionFieldSpecifierKeywords;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.MetricParser;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.apirequest.binders.CountGenerator;
import com.yahoo.bard.webservice.web.apirequest.binders.DimensionGenerator;
import com.yahoo.bard.webservice.web.apirequest.binders.FilterBinders;
import com.yahoo.bard.webservice.web.apirequest.binders.FilterGenerator;
import com.yahoo.bard.webservice.web.apirequest.binders.GranularityGenerator;
import com.yahoo.bard.webservice.web.apirequest.binders.HavingGenerator;
import com.yahoo.bard.webservice.web.apirequest.binders.IntervalGenerationUtils;
import com.yahoo.bard.webservice.web.apirequest.binders.LogicalMetricGenerator;
import com.yahoo.bard.webservice.web.apirequest.binders.LogicalTableGenerator;
import com.yahoo.bard.webservice.web.apirequest.binders.PaginationParameterGenerator;
import com.yahoo.bard.webservice.web.apirequest.binders.ResponseFormatTypeGenerator;
import com.yahoo.bard.webservice.web.apirequest.binders.SortGenerator;
import com.yahoo.bard.webservice.web.apirequest.binders.TopNGenerator;
import com.yahoo.bard.webservice.web.filters.ApiFilters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;

/**
 * Data API Request Implementation binds, validates, and models the parts of a request to the data endpoint.
 */
public class DataApiRequestImpl implements DataApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(DataApiRequestImpl.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    protected static final String SYNCHRONOUS_REQUEST_FLAG = "never";
    protected static final String ASYNCHRONOUS_REQUEST_FLAG = "always";

    private final LogicalTable table;

    private final Granularity granularity;

    private final LinkedHashSet<Dimension> dimensions;
    private final LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields;
    private final LinkedHashSet<LogicalMetric> logicalMetrics;
    private final List<Interval> intervals;
    private final ApiFilters apiFilters;
    private final Map<LogicalMetric, Set<ApiHaving>> havings;
    private final LinkedHashSet<OrderByColumn> sorts;
    private final OrderByColumn dateTimeSort;

    private final int count;
    private final int topN;
    private final DateTimeZone timeZone;

    protected final ResponseFormatType format;
    protected final Optional<PaginationParameters> paginationParameters;
    protected final long asyncAfter;
    protected final String downloadFilename;

    protected FilterGenerator filterGenerator = FilterBinders.getInstance()::generateFilters;

    @Deprecated
    private final DruidFilterBuilder filterBuilder;


    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  string time granularity in URL
     * @param dimensions  single dimension or multiple dimensions separated by '/' in URL
     * @param logicalMetrics  URL logical metric query string in the format:
     * <pre>{@code single metric or multiple logical metrics separated by ',' }</pre>
     * @param intervals  URL intervals query string in the format:
     * <pre>{@code single interval in ISO 8601 format, multiple values separated by ',' }</pre>
     * @param apiFilters  URL filter query String in the format:
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param havings  URL having query String in the format:<pre>
     * {@code
     * ((metric name)-(operation)((values bounded by [])))(followed by , or end of string)
     * }</pre>
     * @param sorts  string of sort columns along with sort direction in the format:<pre>
     * {@code (metricName or dimensionName)|(sortDirection) eg: pageViews|asc }</pre>
     * @param count  count of number of records to be returned in the response
     * @param topN  number of first records per time bucket to be returned in the response
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param timeZoneId  a joda time zone id
     * @param asyncAfter  How long the user is willing to wait for a synchronous request in milliseconds
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param bardConfigResources  The configuration resources used to build this api request
     *
     * @throws BadApiRequestException in the following scenarios:
     * <ol>
     *     <li>Null or empty table name in the API request.</li>
     *     <li>Invalid time grain in the API request.</li>
     *     <li>Invalid dimension in the API request.</li>
     *     <li>Invalid logical metric in the API request.</li>
     *     <li>Invalid interval in the API request.</li>
     *     <li>Invalid filter syntax in the API request.</li>
     *     <li>Invalid filter dimensions in the API request.</li>
     *     <li>Invalid having syntax in the API request.</li>
     *     <li>Invalid having metrics in the API request.</li>
     *     <li>Pagination parameters in the API request that are not positive integers.</li>
     * </ol>
     * @deprecated prefer constructors that use filename parameter instead
     */
    @Deprecated
    public DataApiRequestImpl(
            String tableName,
            String granularity,
            List<PathSegment> dimensions,
            String logicalMetrics,
            String intervals,
            String apiFilters,
            String havings,
            String sorts,
            String count,
            String topN,
            String format,
            String timeZoneId,
            String asyncAfter,
            @NotNull String perPage,
            @NotNull String page,
            BardConfigResources bardConfigResources
    ) throws BadApiRequestException {
        this(
               tableName,
                granularity,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters,
                havings,
                sorts,
                count,
                topN,
                format,
                timeZoneId,
                asyncAfter,
                perPage,
                page,
                bardConfigResources.getDimensionDictionary(),
                bardConfigResources.getMetricDictionary().getScope(Collections.singletonList(tableName)),
                bardConfigResources.getLogicalTableDictionary(),
                bardConfigResources.getSystemTimeZone(),
                bardConfigResources.getGranularityParser(),
                bardConfigResources.getFilterBuilder(),
                bardConfigResources.getHavingApiGenerator()
        );
    }

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  string time granularity in URL
     * @param dimensions  single dimension or multiple dimensions separated by '/' in URL
     * @param logicalMetrics  URL logical metric query string in the format:
     * <pre>{@code single metric or multiple logical metrics separated by ',' }</pre>
     * @param intervals  URL intervals query string in the format:
     * <pre>{@code single interval in ISO 8601 format, multiple values separated by ',' }</pre>
     * @param apiFilters  URL filter query String in the format:
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param havings  URL having query String in the format:<pre>
     * {@code
     * ((metric name)-(operation)((values bounded by [])))(followed by , or end of string)
     * }</pre>
     * @param sorts  string of sort columns along with sort direction in the format:<pre>
     * {@code (metricName or dimensionName)|(sortDirection) eg: pageViews|asc }</pre>
     * @param count  count of number of records to be returned in the response
     * @param topN  number of first records per time bucket to be returned in the response
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param downloadFilename  The filename for the response to be downloaded as. If null indicates response should
     * not be downloaded.
     * @param timeZoneId  a joda time zone id
     * @param asyncAfter  How long the user is willing to wait for a synchronous request in milliseconds
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param bardConfigResources  The configuration resources used to build this api request
     *
     * @throws BadApiRequestException in the following scenarios:
     * <ol>
     *     <li>Null or empty table name in the API request.</li>
     *     <li>Invalid time grain in the API request.</li>
     *     <li>Invalid dimension in the API request.</li>
     *     <li>Invalid logical metric in the API request.</li>
     *     <li>Invalid interval in the API request.</li>
     *     <li>Invalid filter syntax in the API request.</li>
     *     <li>Invalid filter dimensions in the API request.</li>
     *     <li>Invalid having syntax in the API request.</li>
     *     <li>Invalid having metrics in the API request.</li>
     *     <li>Pagination parameters in the API request that are not positive integers.</li>
     * </ol>
     */
    public DataApiRequestImpl(
            String tableName,
            String granularity,
            List<PathSegment> dimensions,
            String logicalMetrics,
            String intervals,
            String apiFilters,
            String havings,
            String sorts,
            String count,
            String topN,
            String format,
            String downloadFilename,
            String timeZoneId,
            String asyncAfter,
            @NotNull String perPage,
            @NotNull String page,
            BardConfigResources bardConfigResources
    ) throws BadApiRequestException {
        this(
               tableName,
                granularity,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters,
                havings,
                sorts,
                count,
                topN,
                format,
                downloadFilename,
                timeZoneId,
                asyncAfter,
                perPage,
                page,
                bardConfigResources.getDimensionDictionary(),
                bardConfigResources.getMetricDictionary().getScope(Collections.singletonList(tableName)),
                bardConfigResources.getLogicalTableDictionary(),
                bardConfigResources.getSystemTimeZone(),
                bardConfigResources.getGranularityParser(),
                bardConfigResources.getFilterBuilder(),
                bardConfigResources.getHavingApiGenerator()
        );
    }

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularityRequest  string time granularity in URL
     * @param dimensionsRequest  single dimension or multiple dimensions separated by '/' in URL
     * @param logicalMetricsRequest  URL logical metric query string in the format:
     * <pre>{@code single metric or multiple logical metrics separated by ',' }</pre>
     * @param intervalsRequest  URL intervals query string in the format:
     * <pre>{@code single interval in ISO 8601 format, multiple values separated by ',' }</pre>
     * @param apiFiltersRequest  URL filter query String in the format:
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param havingsRequest  URL having query String in the format:<pre>
     * {@code
     * ((metric name)-(operation)((values bounded by [])))(followed by , or end of string)
     * }</pre>
     * @param sortsRequest  string of sort columns along with sort direction in the format:<pre>
     * {@code (metricName or dimensionName)|(sortDirection) eg: pageViews|asc }</pre>
     * @param countRequest  count of number of records to be returned in the response
     * @param topNRequest  number of first records per time bucket to be returned in the response
     * @param formatRequest  response data format JSON or CSV. Default is JSON.
     * @param timeZoneId  a joda time zone id
     * @param asyncAfterRequest  How long the user is willing to wait for a synchronous request in milliseconds
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param dimensionDictionary  The dimension dictionary for binding dimensions
     * @param metricDictionary The metric dictionary for binding metrics
     * @param logicalTableDictionary The table dictionary for binding logical tables
     * @param systemTimeZone The default time zone for the system
     * @param granularityParser A tool to process granularities
     * @param druidFilterBuilder A function to build druid filters from Api Filters
     * @param havingGenerator A function to create havings
     *
     * @throws BadApiRequestException in the following scenarios:
     * <ol>
     *     <li>Null or empty table name in the API request.</li>
     *     <li>Invalid time grain in the API request.</li>
     *     <li>Invalid dimension in the API request.</li>
     *     <li>Invalid logical metric in the API request.</li>
     *     <li>Invalid interval in the API request.</li>
     *     <li>Invalid filter syntax in the API request.</li>
     *     <li>Invalid filter dimensions in the API request.</li>
     *     <li>Invalid having syntax in the API request.</li>
     *     <li>Invalid having metrics in the API request.</li>
     *     <li>Pagination parameters in the API request that are not positive integers.</li>
     * </ol>
     * @deprecated prefer constructors that use filename parameter instead
     */
    @Deprecated
    public DataApiRequestImpl(
            String tableName,
            String granularityRequest,
            List<PathSegment> dimensionsRequest,
            String logicalMetricsRequest,
            String intervalsRequest,
            String apiFiltersRequest,
            String havingsRequest,
            String sortsRequest,
            String countRequest,
            String topNRequest,
            String formatRequest,
            String timeZoneId,
            String asyncAfterRequest,
            @NotNull String perPage,
            @NotNull String page,
            DimensionDictionary dimensionDictionary,
            MetricDictionary metricDictionary,
            LogicalTableDictionary logicalTableDictionary,
            DateTimeZone systemTimeZone,
            GranularityParser granularityParser,
            DruidFilterBuilder druidFilterBuilder,
            HavingGenerator havingGenerator
    ) throws BadApiRequestException {
        this(
                tableName,
                granularityRequest,
                dimensionsRequest,
                logicalMetricsRequest,
                intervalsRequest,
                apiFiltersRequest,
                havingsRequest,
                sortsRequest,
                countRequest,
                topNRequest,
                formatRequest,
                null,
                timeZoneId,
                asyncAfterRequest,
                perPage,
                page,
                dimensionDictionary,
                metricDictionary,
                logicalTableDictionary,
                systemTimeZone,
                granularityParser,
                druidFilterBuilder,
                havingGenerator
        );
    }

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularityRequest  string time granularity in URL
     * @param dimensionsRequest  single dimension or multiple dimensions separated by '/' in URL
     * @param logicalMetricsRequest  URL logical metric query string in the format:
     * <pre>{@code single metric or multiple logical metrics separated by ',' }</pre>
     * @param intervalsRequest  URL intervals query string in the format:
     * <pre>{@code single interval in ISO 8601 format, multiple values separated by ',' }</pre>
     * @param apiFiltersRequest  URL filter query String in the format:
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param havingsRequest  URL having query String in the format:<pre>
     * {@code
     * ((metric name)-(operation)((values bounded by [])))(followed by , or end of string)
     * }</pre>
     * @param sortsRequest  string of sort columns along with sort direction in the format:<pre>
     * {@code (metricName or dimensionName)|(sortDirection) eg: pageViews|asc }</pre>
     * @param countRequest  count of number of records to be returned in the response
     * @param topNRequest  number of first records per time bucket to be returned in the response
     * @param formatRequest  response data format JSON or CSV. Default is JSON.
     * @param downloadFilename  The filename for the response to be downloaded as. If null indicates response should
     * not be downloaded.
     * @param timeZoneId  a joda time zone id
     * @param asyncAfterRequest  How long the user is willing to wait for a synchronous request in milliseconds
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param dimensionDictionary  The dimension dictionary for binding dimensions
     * @param metricDictionary The metric dictionary for binding metrics
     * @param logicalTableDictionary The table dictionary for binding logical tables
     * @param systemTimeZone The default time zone for the system
     * @param granularityParser A tool to process granularities
     * @param druidFilterBuilder A function to build druid filters from Api Filters
     * @param havingGenerator A function to create havings
     *
     * @throws BadApiRequestException in the following scenarios:
     * <ol>
     *     <li>Null or empty table name in the API request.</li>
     *     <li>Invalid time grain in the API request.</li>
     *     <li>Invalid dimension in the API request.</li>
     *     <li>Invalid logical metric in the API request.</li>
     *     <li>Invalid interval in the API request.</li>
     *     <li>Invalid filter syntax in the API request.</li>
     *     <li>Invalid filter dimensions in the API request.</li>
     *     <li>Invalid having syntax in the API request.</li>
     *     <li>Invalid having metrics in the API request.</li>
     *     <li>Pagination parameters in the API request that are not positive integers.</li>
     * </ol>
     */
    public DataApiRequestImpl(
            String tableName,
            String granularityRequest,
            List<PathSegment> dimensionsRequest,
            String logicalMetricsRequest,
            String intervalsRequest,
            String apiFiltersRequest,
            String havingsRequest,
            String sortsRequest,
            String countRequest,
            String topNRequest,
            String formatRequest,
            String downloadFilename,
            String timeZoneId,
            String asyncAfterRequest,
            @NotNull String perPage,
            @NotNull String page,
            DimensionDictionary dimensionDictionary,
            MetricDictionary metricDictionary,
            LogicalTableDictionary logicalTableDictionary,
            DateTimeZone systemTimeZone,
            GranularityParser granularityParser,
            DruidFilterBuilder druidFilterBuilder,
            HavingGenerator havingGenerator
    ) throws BadApiRequestException {
        this.format = generateAcceptFormat(formatRequest);

        this.downloadFilename = downloadFilename;

        this.asyncAfter = generateAsyncAfter(
                asyncAfterRequest == null ?
                        SYSTEM_CONFIG.getStringProperty(SYSTEM_CONFIG.getPackageVariableName("default_asyncAfter")) :
                        asyncAfterRequest
        );

        this.paginationParameters = generatePaginationParameters(perPage, page);

        timeZone = generateTimeZone(timeZoneId, systemTimeZone);

        // Time grain must be from allowed interval keywords
        this.granularity = generateGranularity(granularityRequest, timeZone, granularityParser);

        this.table = bindLogicalTable(tableName, granularity, logicalTableDictionary);
        validateLogicalTable(tableName, table, granularity, logicalTableDictionary);

        // Zero or more grouping dimensions may be specified
        this.dimensions = bindGroupingDimensions(dimensionsRequest, table, dimensionDictionary);
        validateGroupingDimensions(dimensionsRequest, dimensions, table, dimensionDictionary);

        // Map of dimension to its fields specified using show clause (matrix params)
        this.perDimensionFields = bindDimensionFields(dimensionsRequest, dimensions, table, dimensionDictionary);
        validateDimensionFields(dimensionsRequest, perDimensionFields, dimensions, table, dimensionDictionary);

        // At least one logical metric is required
        this.filterBuilder = druidFilterBuilder;  // required for intersection metrics to work

        this.logicalMetrics = bindLogicalMetrics(logicalMetricsRequest, table, metricDictionary, dimensionDictionary);
        validateLogicalMetrics(logicalMetricsRequest, logicalMetrics, table, metricDictionary);

        this.intervals = bindIntervals(intervalsRequest, granularity, timeZone);
        validateIntervals(intervalsRequest, intervals, granularity, timeZone);

        // Zero or more filtering dimensions may be referenced
        this.apiFilters = bindApiFilters(apiFiltersRequest, table, dimensionDictionary);
        validateApiFilters(apiFiltersRequest, apiFilters, table, dimensionDictionary);
        validateRequestDimensions(apiFilters.keySet(), table);
        validateAggregatability(dimensions, apiFilters);

        // Zero or more having queries may be referenced
        this.havings = bindApiHavings(havingsRequest, havingGenerator, logicalMetrics);
        validateApiHavings(havingsRequest, havings);

        //Using the LinkedHashMap to preserve the sort order
        LinkedHashMap<String, SortDirection> sortColumnDirection = bindToColumnDirectionMap(sortsRequest);

        //Requested sort on dateTime column
        this.dateTimeSort = bindDateTimeSortColumn(sortColumnDirection).orElse(null);

        // Requested sort on metrics - optional, can be empty Set
        this.sorts = bindToColumnDirectionMap(
                removeDateTimeSortColumn(sortColumnDirection),
                logicalMetrics,
                metricDictionary
        );
        validateSortColumns(sorts, dateTimeSort, sortsRequest, logicalMetrics, metricDictionary);

        // Overall requested number of rows in the response. Ignores grouping in time buckets.
        this.count = bindCount(countRequest);
        validateCount(countRequest, count);

        // TODO use topN generator
        // Requested number of rows per time bucket in the response
        this.topN = bindTopN(topNRequest);
        validateTopN(topNRequest, topN, sorts);

        LOG.debug(
                "Api request: TimeGrain: {}," +
                        " Table: {}," +
                        " Dimensions: {}," +
                        " Dimension Fields: {}," +
                        " Filters: {},\n" +
                        " Havings: {},\n" +
                        " Logical metrics: {},\n\n" +
                        " Sorts: {}," +
                        " Count: {}," +
                        " TopN: {}," +
                        " AsyncAfter: {}" +
                        " Format: {}" +
                        " Pagination: {}",
                granularity,
                table.getName(),
                dimensions,
                perDimensionFields,
                apiFilters,
                havings,
                logicalMetrics,
                sorts,
                count,
                topN,
                asyncAfter,
                format,
                paginationParameters
        );
    }

    /**
     * All argument constructor, meant to be used for rewriting apiRequest.
     *
     * @param format  Format for the response
     * @param paginationParameters  Pagination info
     * @param table  Logical table requested
     * @param granularity  Granularity of the request
     * @param dimensions  Grouping dimensions of the request
     * @param perDimensionFields  Fields for each of the grouped dimensions
     * @param logicalMetrics  Metrics requested
     * @param intervals  Intervals requested
     * @param apiFilters  Global filters
     * @param havings  Top-level Having caluses for the request
     * @param sorts  Sorting info for the request
     * @param count  Global limit for the request
     * @param topN  Count of per-bucket limit (TopN) for the request
     * @param asyncAfter  How long in milliseconds the user is willing to wait for a synchronous response
     * @param timeZone  TimeZone for the request
     * @param filterBuilder  A builder to use when building filters for the request
     * @param dateTimeSort  A dateTime sort column with its direction
     * @deprecated prefer constructor with downloadFilename
     */
    @Deprecated
    protected DataApiRequestImpl(
            ResponseFormatType format,
            Optional<PaginationParameters> paginationParameters,
            LogicalTable table,
            Granularity granularity,
            LinkedHashSet<Dimension> dimensions,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields,
            LinkedHashSet<LogicalMetric> logicalMetrics,
            List<Interval> intervals,
            ApiFilters apiFilters,
            Map<LogicalMetric, Set<ApiHaving>> havings,
            LinkedHashSet<OrderByColumn> sorts,
            int count,
            int topN,
            long asyncAfter,
            DateTimeZone timeZone,
            DruidFilterBuilder filterBuilder,
            Optional<OrderByColumn> dateTimeSort
    ) {
        this(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                logicalMetrics,
                intervals,
                apiFilters,
                havings,
                sorts,
                dateTimeSort,
                timeZone,
                topN,
                count,
                paginationParameters,
                format,
                asyncAfter,
                filterBuilder
        );
    }

    /**
     * All argument constructor, meant to be used for rewriting apiRequest.
     *
     * @param format  Format for the response
     * @param downloadFilename  The filename for the response to be downloaded as. If null indicates response should
     * not be downloaded.
     * @param paginationParameters  Pagination info
     * @param table  Logical table requested
     * @param granularity  Granularity of the request
     * @param dimensions  Grouping dimensions of the request
     * @param perDimensionFields  Fields for each of the grouped dimensions
     * @param logicalMetrics  Metrics requested
     * @param intervals  Intervals requested
     * @param apiFilters  Global filters
     * @param havings  Top-level Having caluses for the request
     * @param sorts  Sorting info for the request
     * @param count  Global limit for the request
     * @param topN  Count of per-bucket limit (TopN) for the request
     * @param asyncAfter  How long in milliseconds the user is willing to wait for a synchronous response
     * @param timeZone  TimeZone for the request
     * @param filterBuilder  A builder to use when building filters for the request
     * @param dateTimeSort  A dateTime sort column with its direction
     */
    protected DataApiRequestImpl(
            ResponseFormatType format,
            String downloadFilename,
            Optional<PaginationParameters> paginationParameters,
            LogicalTable table,
            Granularity granularity,
            LinkedHashSet<Dimension> dimensions,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields,
            LinkedHashSet<LogicalMetric> logicalMetrics,
            List<Interval> intervals,
            ApiFilters apiFilters,
            Map<LogicalMetric, Set<ApiHaving>> havings,
            LinkedHashSet<OrderByColumn> sorts,
            int count,
            int topN,
            long asyncAfter,
            DateTimeZone timeZone,
            DruidFilterBuilder filterBuilder,
            Optional<OrderByColumn> dateTimeSort
    ) {
        this(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                logicalMetrics,
                intervals,
                apiFilters,
                havings,
                sorts,
                dateTimeSort,
                timeZone,
                topN,
                count,
                paginationParameters,
                format,
                downloadFilename,
                asyncAfter,
                filterBuilder
        );
    }

    /**
     * All argument constructor, meant to be used for rewriting apiRequest.
     *
     * Filter builder in constructor should be removed when some deprecations come out.
     *
     * @param format  Format for the response
     * @param paginationParameters  Pagination info
     * @param table  Logical table requested
     * @param granularity  Granularity of the request
     * @param groupingDimensions  Grouping dimensions of the request
     * @param perDimensionFields  Fields for each of the grouped dimensions
     * @param logicalMetrics  Metrics requested
     * @param intervals  Intervals requested
     * @param apiFilters  Global filters
     * @param havings  Top-level Having caluses for the request
     * @param sorts  Sorting info for the request
     * @param dateTimeSort Override sort on time
     * @param count  Global limit for the request
     * @param topN  Count of per-bucket limit (TopN) for the request
     * @param asyncAfter  How long in milliseconds the user is willing to wait for a synchronous response
     * @param timeZone  TimeZone for the request
     * @param filterBuilder  A builder to use when building filters for the request
     * @deprecated prefer constructor with downloadFilename
     */
    @Deprecated
    protected DataApiRequestImpl(
            LogicalTable table,
            Granularity granularity,
            LinkedHashSet<Dimension> groupingDimensions,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields,
            LinkedHashSet<LogicalMetric> logicalMetrics,
            List<Interval> intervals,
            ApiFilters apiFilters,
            Map<LogicalMetric, Set<ApiHaving>> havings,
            LinkedHashSet<OrderByColumn> sorts,
            Optional<OrderByColumn> dateTimeSort,
            DateTimeZone timeZone,
            int topN,
            int count,
            Optional<PaginationParameters> paginationParameters,
            ResponseFormatType format,
            Long asyncAfter,
            DruidFilterBuilder filterBuilder
    ) {
        this(
                table,
                granularity,
                groupingDimensions,
                perDimensionFields,
                logicalMetrics,
                intervals,
                apiFilters,
                havings,
                sorts,
                dateTimeSort,
                timeZone,
                topN,
                count,
                paginationParameters,
                format,
                null,
                asyncAfter,
                filterBuilder
        );
    }

    /**
     * All argument constructor, meant to be used for rewriting apiRequest.
     *
     * Filter builder in constructor should be removed when some deprecations come out.
     *
     * @param format  Format for the response
     * @param downloadFilename  The filename for the response to be downloaded as. If null indicates response should
     * not be downloaded.
     * @param paginationParameters  Pagination info
     * @param table  Logical table requested
     * @param granularity  Granularity of the request
     * @param groupingDimensions  Grouping dimensions of the request
     * @param perDimensionFields  Fields for each of the grouped dimensions
     * @param logicalMetrics  Metrics requested
     * @param intervals  Intervals requested
     * @param apiFilters  Global filters
     * @param havings  Top-level Having caluses for the request
     * @param sorts  Sorting info for the request
     * @param dateTimeSort Override sort on time
     * @param count  Global limit for the request
     * @param topN  Count of per-bucket limit (TopN) for the request
     * @param asyncAfter  How long in milliseconds the user is willing to wait for a synchronous response
     * @param timeZone  TimeZone for the request
     * @param filterBuilder  A builder to use when building filters for the request
     */
    protected DataApiRequestImpl(
            LogicalTable table,
            Granularity granularity,
            LinkedHashSet<Dimension> groupingDimensions,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields,
            LinkedHashSet<LogicalMetric> logicalMetrics,
            List<Interval> intervals,
            ApiFilters apiFilters,
            Map<LogicalMetric, Set<ApiHaving>> havings,
            LinkedHashSet<OrderByColumn> sorts,
            Optional<OrderByColumn> dateTimeSort,
            DateTimeZone timeZone,
            int topN,
            int count,
            Optional<PaginationParameters> paginationParameters,
            ResponseFormatType format,
            String downloadFilename,
            Long asyncAfter,
            DruidFilterBuilder filterBuilder
    ) {
        this.format = format;
        this.downloadFilename = downloadFilename;
        this.asyncAfter = asyncAfter;
        this.paginationParameters = paginationParameters;
        this.table = table;
        this.granularity = granularity;
        this.dimensions = groupingDimensions;
        this.perDimensionFields = perDimensionFields;
        this.logicalMetrics = logicalMetrics;
        this.intervals = intervals;
        this.apiFilters = apiFilters;
        this.havings = havings;
        this.sorts = sorts;
        this.dateTimeSort = dateTimeSort.orElse(null);
        this.count = count;
        this.topN = topN;
        this.timeZone = timeZone;
        this.filterBuilder = filterBuilder;
    }

    // Start of binders and validators

    /**
     * Bind the table name against a Logical table in the table dictionary.
     *
     * @param tableName  Name of the logical table from the query
     * @param granularity  The granularity for this request
     * @param logicalTableDictionary  Dictionary to resolve logical tables against.
     *
     * @return  A logical table
     */
    protected LogicalTable bindLogicalTable(
            String tableName,
            Granularity granularity,
            LogicalTableDictionary logicalTableDictionary
    ) {
        return LogicalTableGenerator.DEFAULT_LOGICAL_TABLE_GENERATOR.generateTable(
                tableName,
                granularity,
                logicalTableDictionary
        );
    }

    /**
     * Bind the table name against a Logical table in the table dictionary.
     *
     * @param tableName  Name of the logical table from the query
     * @param table  The bound logical table for this query
     * @param granularity  The granularity for this request
     * @param logicalTableDictionary  Dictionary to resolve logical tables against.
     *
     * @throws BadApiRequestException if invalid
     */
    protected void validateLogicalTable(
            String tableName,
            LogicalTable table,
            Granularity granularity,
            LogicalTableDictionary logicalTableDictionary
    ) throws BadApiRequestException {
        LogicalTableGenerator.DEFAULT_LOGICAL_TABLE_GENERATOR
                .validateTable(table, tableName, granularity, logicalTableDictionary);
    }

    /**
     * Extracts the list of dimension names from the url dimension path segments and generates a set of dimension
     * objects based on it.
     *
     * @param rawGroupingDimensions  Dimension path segments from the URL.
     * @param logicalTable  The table containing the grouping dimensions
     * @param dimensionDictionary  Dimension dictionary contains the map of valid dimension names and dimension objects.
     *
     * @return Set of dimension objects.
     * @throws BadApiRequestException if an invalid dimension is requested.
     */
    protected LinkedHashSet<Dimension> bindGroupingDimensions(
            List<PathSegment> rawGroupingDimensions,
            LogicalTable logicalTable,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        return generateDimensions(rawGroupingDimensions, dimensionDictionary);
    }

    /**
     * Validates the bound grouping dimensions.
     *
     * @param rawGroupingDimensions  Dimension path segments from the URL.
     * @param groupingDimensions  Bound grouping dimensions.
     * @param logicalTable  The table containing the grouping dimensions
     * @param dimensionDictionary  Dimension dictionary contains the map of valid dimension names and dimension objects.
     *
     * @throws BadApiRequestException if invalid
     */
    protected void validateGroupingDimensions(
            List<PathSegment> rawGroupingDimensions,
            LinkedHashSet<Dimension> groupingDimensions,
            LogicalTable logicalTable,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        validateRequestDimensions(groupingDimensions, logicalTable);
    }

    /**
     * Extracts the list of dimensions from the url dimension path segments and "show" matrix params and generates a map
     * of dimension to dimension fields which needs to be annotated on the response.
     * <p>
     * If no "show" matrix param has been set, it returns the default dimension fields configured for the dimension.
     *
     * @param apiDimensionPathSegments  Path segments for the dimensions
     * @param dimensions  The bound dimensions for this query
     * @param logicalTable  The logical table for this query
     * @param dimensionDictionary  Dimension dictionary to look the dimensions up in
     *
     * @return A map of dimension to requested dimension fields
     */
    protected LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> bindDimensionFields(
            List<PathSegment> apiDimensionPathSegments,
            LinkedHashSet<Dimension> dimensions,
            LogicalTable logicalTable,
            DimensionDictionary dimensionDictionary
    ) {
        return generateDimensionFields(apiDimensionPathSegments, dimensionDictionary);
    }

    /**
     * Validated dimension field objects.
     *
     * @param apiDimensionPathSegments  Path segments for the dimensions
     * @param perDimensionFields  The bound dimension fields for this query
     * @param dimensions  The bound dimensions for this query
     * @param logicalTable  The logical table for this query
     * @param dimensionDictionary  Dimension dictionary to look the dimensions up in
     *
     * @throws BadApiRequestException if invalid
     */

    protected void validateDimensionFields(
            List<PathSegment> apiDimensionPathSegments,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields,
            LinkedHashSet<Dimension> dimensions,
            LogicalTable logicalTable,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        // Extend to implement validation
    }

    /**
     * Extracts the list of metrics from the metric query string and generates a set of LogicalMetrics.
     *
     * @param apiMetricExpression  URL query string containing the metrics separated by ','.
     * @param logicalTable  The logical table for the data request
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects.
     * @param dimensionDictionary  Dimension dictionary is used to construct filters on filtered metrics
     *
     * @return set of bound metric objects
     * @throws BadApiRequestException if the metric dictionary returns a null or if the apiMetricQuery is invalid.
     */

    protected LinkedHashSet<LogicalMetric> bindLogicalMetrics(
            String apiMetricExpression,
            LogicalTable logicalTable,
            MetricDictionary metricDictionary,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        return generateLogicalMetrics(apiMetricExpression, metricDictionary, dimensionDictionary, logicalTable);
    }

    /**
     * Validated bound api filter objects.
     *
     * @param apiMetricExpression  URL query string containing the metrics separated by ','.
     * @param metrics The bound logical metrics
     * @param logicalTable  The logical table for the data request
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects.
     *
     * @throws BadApiRequestException if invalid
     */
    protected void validateLogicalMetrics(
            String apiMetricExpression,
            LinkedHashSet<LogicalMetric> metrics,
            LogicalTable logicalTable,
            MetricDictionary metricDictionary
    ) throws BadApiRequestException {
        validateMetrics(metrics, logicalTable);
    }

    /**
     * Generates api filter objects on the based on the filter query vallue in the request parameters.
     *
     * @param filterQuery  A String description of a filter model
     * @param logicalTable  The logical table for the data request
     * @param dimensionDictionary  DimensionDictionary
     *
     * @return Set of filter objects.
     * @throws BadApiRequestException if the filter query string does not match required syntax, or the filter
     * contains a 'startsWith' or 'contains' operation while the BardFeatureFlag.DATA_STARTS_WITH_CONTAINS_ENABLED is
     * off.
     */
    protected ApiFilters bindApiFilters(
            String filterQuery,
            LogicalTable logicalTable,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        return filterGenerator.generate(filterQuery, logicalTable, dimensionDictionary);
    }

    /**
     * Bind the query interval string to a list of intervals.
     *
     * @param intervalsName  The query string describing the intervals
     * @param granularity  The granularity for this request
     * @param timeZone  The time zone to evaluate interval timestamps in
     *
     * @return  A bound list of intervals for the query
     */
    protected List<Interval> bindIntervals(String intervalsName, Granularity granularity, DateTimeZone timeZone) {
        return IntervalGenerator.DEFAULT_INTERVAL_GENERATOR
                .generateIntervals(intervalsName, granularity, timeZone, getTable());
    }

    /**
     * Bind the query interval string to a list of intervals.
     *
     * @param intervalsName  The query string describing the intervals
     * @param intervals  The bound intervals
     * @param granularity The request granularity
     * @param timeZone  The time zone to evaluate interval timestamps in
     *
     * @throws BadApiRequestException if invalid
     */
    protected void validateIntervals(
            String intervalsName,
            List<Interval> intervals,
            Granularity granularity,
            DateTimeZone timeZone
    ) throws BadApiRequestException {
        IntervalGenerator.DEFAULT_INTERVAL_GENERATOR.validateIntervals(intervalsName, intervals, granularity, timeZone);
    }

    /**
     * Validated bound api filter objects.
     *
     * @param filterQuery  A String description of a filter model
     * @param apiFilters  Bound api filters
     * @param logicalTable  The logical table for the data request
     * @param dimensionDictionary  DimensionDictionary
     *
     * @throws BadApiRequestException if invalid
     */
    protected void validateApiFilters(
            String filterQuery,
            ApiFilters apiFilters,
            LogicalTable logicalTable,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        // Extend to implement validation
    }

    /**
     * Produce a map describing the ApiRequest Havings.
     *
     * @param requestHavings  Request query string describing the having api clause
     * @param havingGenerator  The factory to build ApiHaving instances
     * @param logicalMetrics Logical metrics available for filtering
     *
     * @return The Apihaving instances grouped by metric.
     */
    protected Map<LogicalMetric, Set<ApiHaving>> bindApiHavings(
            String requestHavings,
            HavingGenerator havingGenerator,
            Set<LogicalMetric> logicalMetrics
    ) {
        return havingGenerator.apply(requestHavings, logicalMetrics);
    }

    /**
     * Validated bound api havings.
     *
     * @param requestHavings  Request query string describing the having api clause
     * @param apiHavings  The bound api havings collection
     *
     * @throws BadApiRequestException if invalid
     */
    protected void validateApiHavings(
            String requestHavings,
            Map<LogicalMetric, Set<ApiHaving>> apiHavings
    ) throws BadApiRequestException {
        // No default post bind validations yet
    }


    /**
     * Method to convert sort list to column and direction map.
     *
     * @param sorts  String of sort columns
     *
     * @return LinkedHashMap of columns and their direction. Using LinkedHashMap to preserve the order
     */
    protected LinkedHashMap<String, SortDirection> bindToColumnDirectionMap(String sorts) {
        return SortGenerator.generateSortDirectionMap(sorts);
    }

    /**
     * Generates a Set of OrderByColumn.
     *
     * @param sortDirectionMap  Map of columns and their direction
     * @param logicalMetrics  Set of LogicalMetrics in the query
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects.
     *
     * @return a Set of OrderByColumn
     * @throws BadApiRequestException if the sort clause is invalid.
     */
    protected LinkedHashSet<OrderByColumn> bindToColumnDirectionMap(
            Map<String, SortDirection> sortDirectionMap,
            Set<LogicalMetric> logicalMetrics,
            MetricDictionary metricDictionary
    ) throws BadApiRequestException {
        if (sortDirectionMap == null) {
            return new LinkedHashSet<>();
        }
        LinkedHashMap<String, SortDirection> orderedSortDirection = new LinkedHashMap<>(sortDirectionMap);
        return SortGenerator.DEFAULT_SORT_GENERATOR
                .generateSorts(orderedSortDirection, logicalMetrics, metricDictionary);
    }

    /**
     * Method to generate DateTime sort column from the map of columns and its direction.
     *
     * @param sortColumns  LinkedHashMap of columns and its direction. Using LinkedHashMap to preserve the order
     *
     * @return Instance of OrderByColumn for dateTime
     */
    protected Optional<OrderByColumn> bindDateTimeSortColumn(LinkedHashMap<String, SortDirection> sortColumns) {
        return Optional.ofNullable(SortGenerator.DEFAULT_SORT_GENERATOR.generateDateTimeSort(sortColumns));
    }

    /**
     * Validation for sort columns.
     *
     * @param sorts  The set of sort columns
     * @param dateTimeSort  The sort on the date time column
     * @param sortsRequest  The original text parsed into sorts
     * @param logicalMetrics  The supply of metrics for sort columns
     * @param metricDictionary  The dictionary of metrics being sorted
     */
    protected void validateSortColumns(
            LinkedHashSet<OrderByColumn> sorts,
            OrderByColumn dateTimeSort,
            String sortsRequest,
            LinkedHashSet<LogicalMetric> logicalMetrics,
            MetricDictionary metricDictionary
    ) {
        // Extend this to validate
    }


    /**
     * Bind the user request row limit, if any.
     *
     * @param countRequest The value of the count from the request (if any)
     *
     * @return The requested value, zero if null or empty
     */
    protected int bindCount(String countRequest) {
        return CountGenerator.DEFAULT_COUNT_GENERATOR.generateCount(countRequest);
    }

    /**
     * Confirm count size is non negative.
     *
     * @param countRequest The value of the count from the request (if any)
     * @param count The bound value for the count
     */
    protected void validateCount(String countRequest, int count) {
        CountGenerator.DEFAULT_COUNT_GENERATOR.validateCount(countRequest, count);
    }
    /**
     * Bind the top N bucket size (if any).
     *
     * @param topNRequest The value of the topN bucket size from the request (if any)
     *
     * @return The requested value, zero if null or empty
     */
    protected int bindTopN(String topNRequest) {
        return TopNGenerator.DEFAULT_TOP_N_GENERATOR.generateTopN(topNRequest);
    }

    /**
     * Confirm the top N bucket size (if any) is valid.
     *
     * @param topNRequest The value of the count from the request (if any)
     * @param sorts collection of sorted columns
     * @param topN The bound value for the count
     */
    protected void validateTopN(String topNRequest,  int topN, LinkedHashSet<OrderByColumn> sorts) {
        TopNGenerator.DEFAULT_TOP_N_GENERATOR.validateTopN(topNRequest, topN, this.sorts);
    }


    // Binders and Validators complete

    /**
     * Extracts the list of dimensions from the url dimension path segments and "show" matrix params and generates a map
     * of dimension to dimension fields which needs to be annotated on the response.
     * <p>
     * If no "show" matrix param has been set, it returns the default dimension fields configured for the dimension.
     *
     * @param apiDimensionPathSegments  Path segments for the dimensions
     * @param dimensionDictionary  Dimension dictionary to look the dimensions up in
     *
     * @return A map of dimension to requested dimension fields
     */
    protected LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> generateDimensionFields(
            @NotNull List<PathSegment> apiDimensionPathSegments,
            @NotNull DimensionDictionary dimensionDictionary
    ) {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingDimensionFields")) {
            return apiDimensionPathSegments.stream()
                    .filter(pathSegment -> !pathSegment.getPath().isEmpty())
                    .collect(Collectors.toMap(
                            pathSegment -> dimensionDictionary.findByApiName(pathSegment.getPath()),
                            pathSegment -> bindShowClause(pathSegment, dimensionDictionary),
                            (LinkedHashSet<DimensionField> e, LinkedHashSet<DimensionField> i) ->
                                    StreamUtils.orderedSetMerge(e, i),
                            LinkedHashMap::new
                    ));
        }
    }

    /**
     * Given a path segment, bind the fields specified in it's "show" matrix parameter for the dimension specified in
     * the path segment's path.
     *
     * @param pathSegment  Path segment to bind from
     * @param dimensionDictionary  Dimension dictionary to look the dimension up in
     *
     * @return the set of bound DimensionFields specified in the show clause
     * @throws BadApiRequestException if any of the specified fields are not valid for the dimension
     */
    private LinkedHashSet<DimensionField> bindShowClause(
            PathSegment pathSegment,
            DimensionDictionary dimensionDictionary
    )
            throws BadApiRequestException {
        Dimension dimension = dimensionDictionary.findByApiName(pathSegment.getPath());
        List<String> showFields = pathSegment.getMatrixParameters().entrySet().stream()
                .filter(entry -> entry.getKey().equals("show"))
                .flatMap(entry -> entry.getValue().stream())
                .flatMap(s -> Arrays.stream(s.split(",")))
                .collect(Collectors.toList());

        if (showFields.size() == 1 && showFields.contains(DimensionFieldSpecifierKeywords.ALL.toString())) {
            // Show all fields
            return dimension.getDimensionFields();
        } else if (showFields.size() == 1 && showFields.contains(DimensionFieldSpecifierKeywords.NONE.toString())) {
            // Show no fields
            return new LinkedHashSet<>();
        } else if (!showFields.isEmpty()) {
            // Show the requested fields
            return bindDimensionFields(dimension, showFields);
        } else {
            // Show the default fields
            return dimension.getDefaultDimensionFields();
        }
    }

    /**
     * Given a Dimension and a set of DimensionField names, bind the names to the available dimension fields of the
     * dimension.
     *
     * @param dimension  Dimension to bind the fields for
     * @param showFields  Names of the fields to bind
     *
     * @return the set of DimensionFields for the names
     * @throws BadApiRequestException if any of the names are not dimension fields on the dimension
     */
    private LinkedHashSet<DimensionField> bindDimensionFields(Dimension dimension, List<String> showFields)
            throws BadApiRequestException {
        Map<String, DimensionField> dimensionNameToFieldMap = dimension.getDimensionFields().stream()
                .collect(StreamUtils.toLinkedDictionary(DimensionField::getName));

        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>();
        Set<String> invalidDimensionFields = new LinkedHashSet<>();
        for (String field : showFields) {
            if (dimensionNameToFieldMap.containsKey(field)) {
                dimensionFields.add(dimensionNameToFieldMap.get(field));
            } else {
                invalidDimensionFields.add(field);
            }
        }

        if (!invalidDimensionFields.isEmpty()) {
            LOG.debug(DIMENSION_FIELDS_UNDEFINED.logFormat(invalidDimensionFields, dimension.getApiName()));
            throw new BadApiRequestException(DIMENSION_FIELDS_UNDEFINED.format(
                    invalidDimensionFields,
                    dimension.getApiName()
            ));
        }
        return dimensionFields;
    }

    /**
     * Validate that the request references any non-aggregatable dimensions in a valid way.
     *
     * @param apiDimensions  the set of group by dimensions.
     * @param apiFilters  the set of api filters.
     *
     * @throws BadApiRequestException if a the request violates aggregatability constraints of dimensions.
     */
    protected void validateAggregatability(
            Set<Dimension> apiDimensions, Map<Dimension, Set<ApiFilter>> apiFilters
    ) throws BadApiRequestException {
        // The set of non-aggregatable dimensions requested as group by dimensions
        Set<Dimension> nonAggGroupByDimensions = apiDimensions.stream()
                .filter(StreamUtils.not(Dimension::isAggregatable))
                .collect(Collectors.toSet());

        // Check that out of the non-aggregatable dimensions that are not referenced in the group by set already,
        // none of them is mentioned in a filter with more or less than one value
        boolean isValid = apiFilters.entrySet().stream()
                .filter(entry -> !entry.getKey().isAggregatable())
                .filter(entry -> !nonAggGroupByDimensions.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .noneMatch(valueSet -> valueSet.stream().anyMatch(isNonAggregatableInFilter()));

        if (!isValid) {
            List<String> invalidDimensionsInFilters = apiFilters.entrySet().stream()
                    .filter(entry -> !entry.getKey().isAggregatable())
                    .filter(entry -> !nonAggGroupByDimensions.contains(entry.getKey()))
                    .filter(entry -> entry.getValue().stream().anyMatch(isNonAggregatableInFilter()))
                    .map(Map.Entry::getKey)
                    .map(Dimension::getApiName)
                    .collect(Collectors.toList());

            LOG.debug(NON_AGGREGATABLE_INVALID.logFormat(invalidDimensionsInFilters));
            throw new BadApiRequestException(NON_AGGREGATABLE_INVALID.format(invalidDimensionsInFilters));
        }
    }

    /**
     * To check whether dateTime column request is first one in the sort list or not.
     *
     * @param sortColumns  LinkedHashMap of columns and its direction. Using LinkedHashMap to preserve the order
     *
     * @return True if dateTime column is first one in the sort list. False otherwise
     * @deprecated prefer utilizing a {@link SortGenerator} instead of building sorts using the methods provided by
     * {@link DataApiRequestImpl}
     */
    @Deprecated
    protected Boolean isDateTimeFirstSortField(LinkedHashMap<String, SortDirection> sortColumns) {
        if (sortColumns != null) {
            List<String> columns = new ArrayList<>(sortColumns.keySet());
            return columns.get(0).equals(DATE_TIME_STRING);
        } else {
            return false;
        }
    }

    /**
     * Method to remove the dateTime column from map of columns and its direction.
     *
     * @param sortColumns  map of columns and its direction
     *
     * @return  Map of columns and its direction without dateTime sort column
     */
    protected Map<String, SortDirection> removeDateTimeSortColumn(Map<String, SortDirection> sortColumns) {
        if (sortColumns != null && sortColumns.containsKey(DATE_TIME_STRING)) {
            sortColumns.remove(DATE_TIME_STRING);
            return sortColumns;
        } else {
            return sortColumns;
        }
    }

    /**
     * Validity rules for non-aggregatable dimensions that are only referenced in filters.
     * A query that references a non-aggregatable dimension in a filter without grouping by this dimension, is valid
     * only if the requested dimension field is a key for this dimension and only a single value is requested
     * with an inclusive operator ('in' or 'eq').
     *
     * @return A predicate that determines a given dimension is non aggregatable and also not constrained to one row
     * per result
     */
    protected static Predicate<ApiFilter> isNonAggregatableInFilter() {
        return apiFilter ->
                !apiFilter.getDimensionField().equals(apiFilter.getDimension().getKey()) ||
                        apiFilter.getValues().size() != 1 ||
                        !(
                                apiFilter.getOperation().equals(DefaultFilterOperation.in) ||
                                        apiFilter.getOperation().equals(DefaultFilterOperation.eq)
                        );
    }

    /**
     * Extracts the list of metrics from the url metric query string and generates a set of LogicalMetrics.
     *
     * @param apiMetricQuery  URL query string containing the metrics separated by ','.
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects.
     * @param dimensionDictionary  Dimension dictionary to look the dimension up in
     * @param table  The logical table for the data request
     *
     * @return set of metric objects
     * @throws BadApiRequestException if the metric dictionary returns a null or if the apiMetricQuery is invalid.
     */
    protected LinkedHashSet<LogicalMetric> generateLogicalMetrics(
            String apiMetricQuery,
            MetricDictionary metricDictionary,
            DimensionDictionary dimensionDictionary,
            LogicalTable table
    ) throws BadApiRequestException {
        try (TimedPhase timer = RequestLog.startTiming("GeneratingLogicalMetrics")) {
            LOG.trace("Metric dictionary: {}", metricDictionary);

            if (apiMetricQuery == null) {
                apiMetricQuery = "";
            }

            if (BardFeatureFlag.REQUIRE_METRICS_QUERY.isOn() && apiMetricQuery.isEmpty()) {
                LOG.debug(METRICS_MISSING.logFormat());
                throw new BadApiRequestException(METRICS_MISSING.format());
            }

            // set of logical metric objects
            LinkedHashSet<LogicalMetric> generated = new LinkedHashSet<>();
            List<String> invalidMetricNames = new ArrayList<>();

            //If INTERSECTION_REPORTING_ENABLED flag is true, convert the aggregators into FilteredAggregators and
            //replace old PostAggs with new postAggs in order to generate a new Filtered Logical Metric
            if (BardFeatureFlag.INTERSECTION_REPORTING.isOn()) {
                ArrayNode metricsJsonArray;
                try {
                    //For a given metricString, returns an array of json objects contains metric name and associated
                    // filters

                    metricsJsonArray = MetricParser.generateMetricFilterJsonArray(apiMetricQuery);
                } catch (IllegalArgumentException e) {
                    LOG.debug(INCORRECT_METRIC_FILTER_FORMAT.logFormat(e.getMessage()));
                    throw new BadApiRequestException(INCORRECT_METRIC_FILTER_FORMAT.format(apiMetricQuery));
                }
                //check for the duplicate occurrence of metrics in an API
                FieldConverterSupplier.getMetricsFilterSetBuilder().validateDuplicateMetrics(metricsJsonArray);
                for (int i = 0; i < metricsJsonArray.size(); i++) {
                    JsonNode jsonObject;
                    try {
                        jsonObject = metricsJsonArray.get(i);
                    } catch (IndexOutOfBoundsException e) {
                        LOG.debug(INCORRECT_METRIC_FILTER_FORMAT.logFormat(e.getMessage()));
                        throw new BadApiRequestException(INCORRECT_METRIC_FILTER_FORMAT.format(apiMetricQuery));
                    }
                    String metricName = jsonObject.get("name").asText();
                    LogicalMetric logicalMetric = metricDictionary.get(metricName);

                    // If metric dictionary returns a null, it means the requested metric is not found.
                    if (logicalMetric == null) {
                        invalidMetricNames.add(metricName);
                    } else {
                        //metricFilterObject contains all the filters for a given metric
                        JsonNode metricFilterObject = jsonObject.get("filter");

                        //Currently supporting AND operation for metric filters.
                        if (metricFilterObject.has("AND") && !metricFilterObject.get("AND").isNull()) {

                            //We currently do not support ratio metrics
                            if (logicalMetric.getCategory().equals(RATIO_METRIC_CATEGORY)) {
                                LOG.debug(
                                        UNSUPPORTED_FILTERED_METRIC_CATEGORY.logFormat(
                                                logicalMetric.getName(),
                                                logicalMetric.getCategory()
                                        )
                                );
                                throw new BadApiRequestException(
                                        UNSUPPORTED_FILTERED_METRIC_CATEGORY.format(
                                                logicalMetric.getName(),
                                                logicalMetric.getCategory()
                                        )
                                );
                            }
                            try {
                                logicalMetric = FieldConverterSupplier.getMetricsFilterSetBuilder()
                                        .getFilteredLogicalMetric(
                                            logicalMetric,
                                            metricFilterObject,
                                            dimensionDictionary,
                                            table,
                                            this
                                );
                            } catch (FilterBuilderException filterBuilderException) {
                                LOG.debug(filterBuilderException.getMessage());
                                throw new BadApiRequestException(
                                        filterBuilderException.getMessage(),
                                        filterBuilderException
                                );
                            }

                            //If metric filter isn't empty or it has anything other then 'AND' then throw an exception
                        } else if (!metricFilterObject.asText().isEmpty()) {
                            LOG.debug(INVALID_METRIC_FILTER_CONDITION.logFormat(metricFilterObject.asText()));
                            throw new BadApiRequestException(
                                    INVALID_METRIC_FILTER_CONDITION.format(metricFilterObject.asText())
                            );
                        }
                        generated.add(logicalMetric);
                    }
                }

                if (!invalidMetricNames.isEmpty()) {
                    String message = ErrorMessageFormat.METRICS_UNDEFINED.logFormat(invalidMetricNames);
                    LOG.error(message);
                    throw new BadApiRequestException(message);
                }
            } else {
                //Feature flag for intersection reporting is disabled
                // list of metrics extracted from the query string
                generated = generateLogicalMetrics(apiMetricQuery, metricDictionary);
            }
            LOG.trace("Generated set of logical metric: {}", generated);
            return generated;
        }
    }

    /**
     * Generate current date based on granularity.
     *
     * @param dateTime  The current moment as a DateTime
     * @param timeGrain  The time grain used to round the date time
     *
     * @return truncated current date based on granularity
     */
    protected DateTime getCurrentDate(DateTime dateTime, TimeGrain timeGrain) {
        return timeGrain.roundFloor(dateTime);
    }

    /**
     * Extract valid sort direction.
     *
     * @param columnWithDirection  Column and its sorting direction
     *
     * @return Sorting direction. If no direction provided then the default one will be DESC
     */
    protected SortDirection getSortDirection(List<String> columnWithDirection) {
        try {
            return columnWithDirection.size() == 2 ?
                    SortDirection.valueOf(columnWithDirection.get(1).toUpperCase(Locale.ENGLISH)) :
                    SortDirection.DESC;
        } catch (IllegalArgumentException ignored) {
            String sortDirectionName = columnWithDirection.get(1);
            LOG.debug(SORT_DIRECTION_INVALID.logFormat(sortDirectionName));
            throw new BadApiRequestException(SORT_DIRECTION_INVALID.format(sortDirectionName));
        }
    }

    /**
     * Parses the requested input String by converting it to an integer, while treating null as zero.
     *
     * @param value  The requested integer value as String.
     * @param parameterName  The parameter name that corresponds to the requested integer value.
     *
     * @return The integer corresponding to {@code value} or zero if {@code value} is null.
     * @throws BadApiRequestException if the input String can not be parsed as an integer.
     */
    protected int generateInteger(String value, String parameterName) throws BadApiRequestException {
        try {
            return value == null ? 0 : Integer.parseInt(value);
        } catch (NumberFormatException nfe) {
            LOG.debug(INTEGER_INVALID.logFormat(value, parameterName), nfe);
            throw new BadApiRequestException(INTEGER_INVALID.logFormat(value, parameterName), nfe);
        }
    }

    // ************************************************************************************
    // Duplicate methods from ApiRequestImpl that may be used by subclasses of this class.
    // ************************************************************************************
    /**
     * Generate a Granularity instance based on a path element.
     *
     * @param granularity  A string representation of the granularity
     * @param dateTimeZone  The time zone to use for this granularity
     * @param granularityParser  The parser for granularity
     *
     * @return A granularity instance with time zone information
     * @throws BadApiRequestException if the string matches no meaningful granularity
     */
    protected Granularity generateGranularity(
            @NotNull String granularity,
            @NotNull DateTimeZone dateTimeZone,
            @NotNull GranularityParser granularityParser
    ) throws BadApiRequestException {
        return GranularityGenerator.DEFAULT_GRANULARITY_GENERATOR.generateGranularity(
                granularity,
                dateTimeZone,
                granularityParser
        );
    }

    /**
     * Generate a Granularity instance based on a path element.
     *
     * @param granularity  A string representation of the granularity
     * @param granularityParser  The parser for granularity
     *
     * @return A granularity instance without time zone information
     * @throws BadApiRequestException if the string matches no meaningful granularity
     */
    protected Granularity generateGranularity(String granularity, GranularityParser granularityParser)
            throws BadApiRequestException {
        return GranularityGenerator.DEFAULT_GRANULARITY_GENERATOR.generateGranularity(granularity, granularityParser);
    }

    /**
     * Extracts the list of dimension names from the url dimension path segments and generates a set of dimension
     * objects based on it.
     *
     * @param apiDimensions  Dimension path segments from the URL.
     * @param dimensionDictionary  Dimension dictionary contains the map of valid dimension names and dimension objects.
     *
     * @return Set of dimension objects.
     * @throws BadApiRequestException if an invalid dimension is requested.
     */
    protected LinkedHashSet<Dimension> generateDimensions(
            List<PathSegment> apiDimensions,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        return DimensionGenerator.DEFAULT_DIMENSION_GENERATOR.generateDimensions(apiDimensions, dimensionDictionary);
    }

    /**
     * Ensure all request dimensions are part of the logical table.
     *
     * @param requestDimensions  The dimensions being requested
     * @param table  The logical table being checked
     *
     * @throws BadApiRequestException if any of the dimensions do not match the logical table
     */
    protected void validateRequestDimensions(Set<Dimension> requestDimensions, LogicalTable table)
            throws BadApiRequestException {
        DimensionGenerator.DEFAULT_DIMENSION_GENERATOR.validateRequestDimensions(requestDimensions, table);
    }

    /**
     * Given a single dimension filter string, generate a metric name extension.
     *
     * @param filterString  Single dimension filter string.
     *
     * @return Metric name extension created for the filter.
     */
    protected String generateMetricName(String filterString) {
        return LogicalMetricGenerator.generateMetricName(filterString);
    }

    /**
     * Extracts the list of metrics from the url metric query string and generates a set of LogicalMetrics.
     * <p>
     * If the query contains undefined metrics, {@link com.yahoo.bard.webservice.web.BadApiRequestException} will be
     * thrown.
     *
     * @param apiMetricQuery  URL query string containing the metrics separated by ','
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects
     *
     * @return set of metric objects
     */
    protected LinkedHashSet<LogicalMetric> generateLogicalMetrics(
            String apiMetricQuery,
            MetricDictionary metricDictionary
    ) {
        return LogicalMetricGenerator.DEFAULT_LOGICAL_METRIC_GENERATOR.generateLogicalMetrics(
                apiMetricQuery,
                metricDictionary
        );
    }

    /**
     * Validate that all metrics are part of the logical table.
     *
     * @param logicalMetrics  The set of metrics being validated
     * @param table  The logical table for the request
     *
     * @throws BadApiRequestException if the requested metrics are not in the logical table
     */
    protected void validateMetrics(Set<LogicalMetric> logicalMetrics, LogicalTable table)
            throws BadApiRequestException {
        LogicalMetricGenerator.DEFAULT_LOGICAL_METRIC_GENERATOR.validateMetrics(logicalMetrics, table);
    }

    /**
     * Extracts the set of intervals from the api request.
     *
     * @param apiIntervalQuery  API string containing the intervals in ISO 8601 format, values separated by ','.
     * @param granularity  The granularity to generate the date based on period or macros.
     * @param dateTimeFormatter  The formatter to parse date time interval segments
     *
     * @return Set of jodatime interval objects.
     * @throws BadApiRequestException if the requested interval is not found.
     */
    protected static List<Interval> generateIntervals(
            String apiIntervalQuery,
            Granularity granularity,
            DateTimeFormatter dateTimeFormatter
    ) throws BadApiRequestException {
        return IntervalGenerationUtils
                .generateIntervals(new DateTime(), apiIntervalQuery, granularity, dateTimeFormatter);
    }

    /**
     * Extracts the set of intervals from the api request.
     *
     * @param now The 'now' for which time macros will be relatively calculated
     * @param apiIntervalQuery  API string containing the intervals in ISO 8601 format, values separated by ','.
     * @param granularity  The granularity to generate the date based on period or macros.
     * @param dateTimeFormatter  The formatter to parse date time interval segments
     *
     * @return Set of jodatime interval objects.
     * @throws BadApiRequestException if the requested interval is not found.
     */
    protected static List<Interval> generateIntervals(
            DateTime now,
            String apiIntervalQuery,
            Granularity granularity,
            DateTimeFormatter dateTimeFormatter
    ) throws BadApiRequestException {
        return IntervalGenerationUtils.generateIntervals(now, apiIntervalQuery, granularity, dateTimeFormatter);
    }

    /**
     * Get datetime from the given input text based on granularity.
     *
     * @param now  current datetime to compute the floored date based on granularity
     * @param granularity  granularity to truncate the given date to.
     * @param dateText  start/end date text which could be actual date or macros
     * @param timeFormatter  a time zone adjusted date time formatter
     *
     * @return joda datetime of the given start/end date text or macros
     *
     * @throws BadApiRequestException if the granularity is "all" and a macro is used
     */
    public static DateTime getAsDateTime(
            DateTime now,
            Granularity granularity,
            String dateText,
            DateTimeFormatter timeFormatter
    ) throws BadApiRequestException {
        return IntervalGenerationUtils.getAsDateTime(now, granularity, dateText, timeFormatter);
    }

    /**
     * Get the timezone for the request.
     *
     * @param timeZoneId  String of the TimeZone ID
     * @param systemTimeZone  TimeZone of the system to use if there is no timeZoneId
     *
     * @return the request's TimeZone
     */
    protected DateTimeZone generateTimeZone(String timeZoneId, DateTimeZone systemTimeZone) {
        try (TimedPhase timer = RequestLog.startTiming("generatingTimeZone")) {
            if (timeZoneId == null) {
                return systemTimeZone;
            }
            try {
                return DateTimeZone.forID(timeZoneId);
            } catch (IllegalArgumentException ignored) {
                LOG.debug(INVALID_TIME_ZONE.logFormat(timeZoneId));
                throw new BadApiRequestException(INVALID_TIME_ZONE.format(timeZoneId));
            }
        }
    }

    /**
     * Throw an exception if any of the intervals are not accepted by this granularity.
     *
     * @param granularity  The granularity whose alignment is being tested.
     * @param intervals  The intervals being tested.
     *
     * @throws BadApiRequestException if the granularity does not align to the intervals
     */
    protected static void validateTimeAlignment(
            Granularity granularity,
            List<Interval> intervals
    ) throws BadApiRequestException {
        IntervalGenerationUtils.validateTimeAlignment(granularity, intervals);
    }

    /**
     * Generates the format in which the response data is expected.
     *
     * @param format  Expects a URL format query String.
     *
     * @return Response format type (CSV or JSON).
     * @throws BadApiRequestException if the requested format is not found.
     */
    protected ResponseFormatType generateAcceptFormat(String format) throws BadApiRequestException {
        return ResponseFormatTypeGenerator.DEFAULT_RESPONSE_FORMAT_TYPE_GENERATOR.generateAcceptFormat(format);
    }

    /**
     * Builds the paginationParameters object, if the request provides both a perPage and page field.
     *
     * @param perPage  The number of rows per page.
     * @param page  The page to display.
     *
     * @return An Optional wrapping a PaginationParameters if both 'perPage' and 'page' exist.
     * @throws BadApiRequestException if 'perPage' or 'page' is not a positive integer, or if either one is empty
     * string but not both.
     */
    protected Optional<PaginationParameters> generatePaginationParameters(String perPage, String page)
            throws BadApiRequestException {
        return PaginationParameterGenerator.DEFAULT_PAGINATION_PARAMETER_GENERATOR
                .generatePaginationParameters(perPage, page);
    }

    /**
     * Extracts a specific logical table object given a valid table name and a valid granularity.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  logical table corresponding to the table name specified in the URL
     * @param logicalTableDictionary  Logical table dictionary contains the map of valid table names and table objects.
     *
     * @return Set of logical table objects.
     * @throws BadApiRequestException Invalid table exception if the table dictionary returns a null.
     */
    protected LogicalTable generateTable(
            String tableName,
            Granularity granularity,
            LogicalTableDictionary logicalTableDictionary
    ) throws BadApiRequestException {
        return LogicalTableGenerator.DEFAULT_LOGICAL_TABLE_GENERATOR.generateTable(
                tableName,
                granularity,
                logicalTableDictionary
        );
    }

    /**
     * This method returns a Function that can basically take a Collection and return an instance of
     * AllPagesPagination.
     *
     * @param paginationParameters  The PaginationParameters to be used to generate AllPagesPagination instance
     * @param <T>  The type of items in the Collection which needs to be paginated
     *
     * @return A Function that takes a Collection and returns an instance of AllPagesPagination
     */
    public <T> Function<Collection<T>, AllPagesPagination<T>> getAllPagesPaginationFactory(
            PaginationParameters paginationParameters
    ) {
        return data -> new AllPagesPagination<>(data, paginationParameters);
    }

    /**
     * Parses the asyncAfter parameter into a long describing how long the user is willing to wait for the results of a
     * synchronous request before the request should become asynchronous.
     *
     * @param asyncAfterString  asyncAfter should be either a string representation of a long, or the String never
     *
     * @return A long describing how long the user is willing to wait
     *
     * @throws BadApiRequestException if asyncAfterString is neither the string representation of a natural number, nor
     * {@code never}
     */
    protected long generateAsyncAfter(String asyncAfterString) throws BadApiRequestException {
        try {
            return asyncAfterString.equals(SYNCHRONOUS_REQUEST_FLAG) ?
                    SYNCHRONOUS_ASYNC_AFTER_VALUE :
                    asyncAfterString.equals(ASYNCHRONOUS_REQUEST_FLAG) ?
                            ASYNCHRONOUS_ASYNC_AFTER_VALUE :
                            Long.parseLong(asyncAfterString);
        } catch (NumberFormatException e) {
            LOG.debug(INVALID_ASYNC_AFTER.logFormat(asyncAfterString), e);
            throw new BadApiRequestException(INVALID_ASYNC_AFTER.format(asyncAfterString), e);
        }
    }

    /**
     * Get a filter generator binder for writing ApiFilters.
     *
     * @return  An implementation of FilterGenerator.
     *
     * @deprecated Remove when removing {@link #generateFilters(String, LogicalTable, DimensionDictionary)}
     */
    @Deprecated
    protected FilterGenerator getFilterGenerator() {
        return FilterBinders.getInstance()::generateFilters;
    }

    // CHECKSTYLE:OFF
    @Override
    public DataApiRequestImpl withFormat(ResponseFormatType format) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequest withDownloadFilename(String downloadFilename) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequestImpl withPaginationParameters(Optional<PaginationParameters> paginationParameters) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequestImpl withBuilder(Response.ResponseBuilder builder) {
        return this;
    }

    @Override
    public DataApiRequestImpl withTable(LogicalTable table) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequestImpl withGranularity(Granularity granularity) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequestImpl withDimensions(LinkedHashSet<Dimension> dimensions) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequestImpl withPerDimensionFields(LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequestImpl withLogicalMetrics(LinkedHashSet<LogicalMetric> logicalMetrics) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequestImpl withIntervals(List<Interval> intervals) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    @Deprecated
    public DataApiRequestImpl withIntervals(Set<Interval> intervals) {
        return withIntervals(new ArrayList<>(intervals));
    }

    @Override
    public DataApiRequestImpl withFilters(ApiFilters apiFilters) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequestImpl withHavings(Map<LogicalMetric, Set<ApiHaving>> havings) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequestImpl withSorts(LinkedHashSet<OrderByColumn> sorts) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    // TODO
    @Override
    public DataApiRequestImpl withTimeSort(Optional<OrderByColumn> timeSort) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequestImpl withCount(int count) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequestImpl withTopN(int topN) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequestImpl withAsyncAfter(long asyncAfter) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequestImpl withTimeZone(DateTimeZone timeZone) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    @Override
    public DataApiRequestImpl withFilterBuilder(DruidFilterBuilder filterBuilder) {
        return new DataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, filterBuilder);
    }

    // CHECKSTYLE:ON

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
        return this.dimensions;
    }

    @Override
    public LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> getDimensionFields() {
        return this.perDimensionFields;
    }

    @Override
    public Set<LogicalMetric> getLogicalMetrics() {
        return this.logicalMetrics;
    }

    @Override
    public List<Interval> getIntervals() {
        return this.intervals;
    }

    @Override
    public ApiFilters getApiFilters() {
        return this.apiFilters;
    }

    @Override
    @Deprecated
    public Map<Dimension, Set<ApiFilter>> generateFilters(
            final String filterQuery, final LogicalTable table, final DimensionDictionary dimensionDictionary
    ) {
        return filterGenerator.generate(filterQuery, table, dimensionDictionary);
    }

    @Override
    @Deprecated
    public Filter getQueryFilter() {
        try (TimedPhase timer = RequestLog.startTiming("BuildingDruidFilter")) {
            return filterBuilder.buildFilters(this.apiFilters);
        } catch (FilterBuilderException e) {
            LOG.debug(e.getMessage());
            throw new BadApiRequestException(e);
        }
    }

    @Override
    @Deprecated
    public DruidFilterBuilder getFilterBuilder() {
        return filterBuilder;
    }

    @Override
    public Map<LogicalMetric, Set<ApiHaving>> getHavings() {
        return this.havings;
    }

    @Override
    @Deprecated
    public Having getQueryHaving() {
        return DefaultDruidHavingBuilder.INSTANCE.buildHavings(havings);
    }

    @Override
    public LinkedHashSet<OrderByColumn> getSorts() {
        return this.sorts;
    }

    @Override
    public Optional<OrderByColumn> getDateTimeSort() {
        return Optional.ofNullable(dateTimeSort);
    }

    @Override
    public OptionalInt getCount() {
        return count == 0 ? OptionalInt.empty() : OptionalInt.of(count);
    }

    @Override
    public OptionalInt getTopN() {
        return topN == 0 ? OptionalInt.empty() : OptionalInt.of(topN);
    }

    @Override
    public DateTimeZone getTimeZone() {
        return timeZone;
    }

    // NEW METHODS HERE

    @Override
    public ResponseFormatType getFormat() {
        return format;
    }

    @Override
    public Optional<PaginationParameters> getPaginationParameters() {
        return paginationParameters;
    }

    @Override
    public Long getAsyncAfter() {
        return asyncAfter;
    }

    @Override
    public Optional<String> getDownloadFilename() {
        return Optional.ofNullable(downloadFilename);
    }

    @Deprecated
    public PaginationParameters getDefaultPagination() {
        return DEFAULT_PAGINATION;
    }
}
