// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTEGER_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TOP_N_UNSORTED;
import static com.yahoo.bard.webservice.web.apirequest.DefaultSortColumnGenerators.DATE_TIME_COLUMN_NAME;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.FilterBuilderException;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.druid.model.builders.DefaultDruidHavingBuilder;
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder;
import com.yahoo.bard.webservice.druid.model.builders.DruidOrFilterBuilder;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.util.TableUtils;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.ApiHaving;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.apirequest.binders.DefaultTableBinder;
import com.yahoo.bard.webservice.web.apirequest.binders.FilterBinders;
import com.yahoo.bard.webservice.web.apirequest.binders.FilterGenerator;
import com.yahoo.bard.webservice.web.apirequest.binders.HavingGenerator;
import com.yahoo.bard.webservice.web.filters.ApiFilters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;

/**
 * Data API Request Implementation binds, validates, and models the parts of a request to the data endpoint.
 */
public class DataApiRequestImpl extends ApiRequestImpl implements DataApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(DataApiRequestImpl.class);

    private final LogicalTable table;

    private final Granularity granularity;

    private final LinkedHashSet<Dimension> groupingDimensions;
    private final LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields;
    private final LinkedHashSet<LogicalMetric> logicalMetrics;
    private final List<Interval> intervals;
    private final ApiFilters apiFilters;
    private final Map<LogicalMetric, Set<ApiHaving>> havings;
    private final LinkedHashSet<OrderByColumn> sorts;
    private final OrderByColumn dateTimeSort;

    private final Integer count;
    private final Integer topN;
    private final DateTimeZone timeZone;

    // binders
    DefaultTableBinder defaultTableBinders = new DefaultTableBinder();


    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * Delegates to unpacked constructor with a null valued filename.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularityRequest  string time granularity in URL
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
     * @param containerRequest  The container request
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
     *
     * @deprecated prefer constructors that use filename parameter instead
     */
    @Deprecated
    public DataApiRequestImpl(
            String tableName,
            String granularityRequest,
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
            ContainerRequestContext containerRequest,
            BardConfigResources bardConfigResources
    ) throws BadApiRequestException {
        this(
               tableName,
                granularityRequest,
                dimensions,
                logicalMetrics,
                intervals,
                apiFilters,
                havings,
                sorts,
                count,
                topN,
                format,
                null,
                timeZoneId,
                asyncAfter,
                perPage,
                page,
                bardConfigResources.getDimensionDictionary(),
                bardConfigResources.getMetricDictionary().getScope(Collections.singletonList(tableName)),
                bardConfigResources.getLogicalTableDictionary(),
                bardConfigResources.getSystemTimeZone(),
                bardConfigResources.getGranularityParser(),
                bardConfigResources.getHavingApiGenerator()
        );
    }

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * Default filename to null.
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
     * @param containerRequest  The container request
     * @param dimensionDictionary  The dimension dictionary for binding dimensions
     * @param metricDictionary The metric dictionary for binding metrics
     * @param logicalTableDictionary The table dictionary for binding logical tables
     * @param systemTimeZone The default time zone for the system
     * @param granularityParser A tool to process granularities
     * @param druidFilterBuilder A function to build druid filters from Api Filters
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
     *
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
            ContainerRequestContext containerRequest,
            DimensionDictionary dimensionDictionary,
            MetricDictionary metricDictionary,
            LogicalTableDictionary logicalTableDictionary,
            DateTimeZone systemTimeZone,
            GranularityParser granularityParser,
            DruidFilterBuilder druidFilterBuilder,
            HavingGenerator havingsApiParser
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
                havingsApiParser
        );
    }

    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * Delegate to building constructor discarding vestigial druidFilterBuilder.
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
     * @param havingsApiParser A function to create havings
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
            HavingGenerator havingsApiParser
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
                downloadFilename,
                timeZoneId,
                asyncAfterRequest,
                perPage,
                page,
                dimensionDictionary,
                metricDictionary,
                logicalTableDictionary,
                systemTimeZone,
                granularityParser,
                havingsApiParser
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
            HavingGenerator havingGenerator
    ) throws BadApiRequestException {
        super(formatRequest, downloadFilename, asyncAfterRequest, perPage, page);

        timeZone = bindTimeZone(timeZoneId, systemTimeZone);

        // Time grain must be from allowed interval keywords
        this.granularity = bindGranularity(granularityRequest, timeZone, granularityParser);

        TableIdentifier tableId = new TableIdentifier(tableName, this.granularity);

        this.table = bindLogicalTable(tableName, granularity, logicalTableDictionary);
        validateLogicalTable(tableName, table, granularity, logicalTableDictionary);

        // Zero or more grouping dimensions may be specified
        this.groupingDimensions = bindGroupingDimensions(dimensionsRequest, table, dimensionDictionary);
        validateGroupingDimensions(dimensionsRequest, groupingDimensions, table, dimensionDictionary);

        // Map of dimension to its fields specified using show clause (matrix params)
        this.perDimensionFields = bindDimensionFields(
                dimensionsRequest,
                groupingDimensions,
                table,
                dimensionDictionary
        );
        validateDimensionFields(dimensionsRequest, perDimensionFields, groupingDimensions, table, dimensionDictionary);

        this.logicalMetrics = bindLogicalMetrics(logicalMetricsRequest, table, metricDictionary, dimensionDictionary);
        // At least one logical metric is required
        validateLogicalMetrics(logicalMetricsRequest, logicalMetrics, table, metricDictionary);

        this.intervals = bindIntervals(intervalsRequest, granularity, timeZone);
        validateIntervals(intervalsRequest, intervals, granularity, timeZone);

        // Zero or more filtering dimensions may be referenced
        this.apiFilters = bindApiFilters(apiFiltersRequest, table, dimensionDictionary);
        validateApiFilters(apiFiltersRequest, apiFilters, table, dimensionDictionary);
        validateRequestDimensions(apiFilters.keySet(), table);


        // Zero or more having queries may be referenced
        this.havings = bindApiHavings(havingsRequest, havingGenerator, logicalMetrics);
        validateApiHavings(havingsRequest, havings);

        //Using the LinkedHashMap to preserve the sort order
        LinkedHashMap<String, SortDirection> sortColumnDirection = bindToColumnDirectionMap(sortsRequest);

        //Requested sort on dateTime column
        this.dateTimeSort = bindDateTimeSortColumn(sortColumnDirection).orElse(null);

        // Requested sort on metrics - optional, can be empty Set
        this.sorts = bindSorts(
                removeDateTimeSortColumn(sortColumnDirection),
                logicalMetrics, metricDictionary
        );
        validateSortColumns(sorts, dateTimeSort, sortsRequest, logicalMetrics, metricDictionary);

        // Overall requested number of rows in the response. Ignores grouping in time buckets.
        this.count = bindCount(countRequest);
        validateCount(countRequest, count);

        // Requested number of rows per time bucket in the response
        this.topN = bindTopN(topNRequest);
        validateTopN(topNRequest, topN, sorts);

        validateAggregatability(this.groupingDimensions, this.apiFilters);

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
                groupingDimensions,
                perDimensionFields,
                apiFilters,
                havings,
                logicalMetrics,
                sorts,
                count,
                topN,
                asyncAfter,
                format,
                getPaginationParameters()
        );
        validateAggregatability(this.groupingDimensions, this.apiFilters);
        ApiRequestValidators.INSTANCE.validateTimeAlignment(this.granularity, this.intervals);
    }


    /**
     * All argument constructor, meant to be used for rewriting apiRequest.
     *
     * Filter builder in constructor should be removed when some deprecations come out.
     *
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
     * @param timeZone  TimeZone for the request
     * @param topN  Count of per-bucket limit (TopN) for the request
     * @param count  Global limit for the request
     * @param format  Format for the response
     * @param paginationParameters  Pagination info
     * @param asyncAfter  How long in milliseconds the user is willing to wait for a synchronous response
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
            PaginationParameters paginationParameters,
            ResponseFormatType format,
            String downloadFilename,
            Long asyncAfter
    ) {
        super(format, downloadFilename, asyncAfter, paginationParameters);
        this.table = table;
        this.granularity = granularity;
        this.groupingDimensions = new LinkedHashSet<>(groupingDimensions);
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
        return defaultTableBinders.bindLogicalTable(tableName, granularity, logicalTableDictionary);
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
        defaultTableBinders.validateLogicalTable(tableName, table, granularity, logicalTableDictionary);
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
        return DefaultDimensionGenerators.INSTANCE.generateDimensions(rawGroupingDimensions, dimensionDictionary);
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
        ApiRequestValidators.INSTANCE.validateRequestDimensions(groupingDimensions, logicalTable);
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
        return DefaultDimensionGenerators.INSTANCE.generateDimensionFields(apiDimensionPathSegments, dimensionDictionary);
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
        ;
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
        return DefaultLogicalMetricsGenerators.INSTANCE.generateLogicalMetrics(
                apiMetricExpression,
                metricDictionary,
                dimensionDictionary,
                logicalTable
        );
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
    ApiFilters bindApiFilters(String filterQuery, LogicalTable logicalTable, DimensionDictionary dimensionDictionary)
            throws BadApiRequestException {
        return FilterBinders.INSTANCE.generateFilters(filterQuery, logicalTable, dimensionDictionary);
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
     * Bind the query interval string to a list of intervals.
     *
     * @param intervalsName  The query string describing the intervals
     * @param granularity  The granularity for this request
     * @param timeZone  The time zone to evaluate interval timestamps in
     *
     * @return  A bound list of intervals for the query
     */
    protected List<Interval> bindIntervals(String intervalsName, Granularity granularity, DateTimeZone timeZone) {
        DateTimeFormatter dateTimeFormatter = generateDateTimeFormatter(timeZone);
        List<Interval> result;

        if (BardFeatureFlag.CURRENT_MACRO_USES_LATEST.isOn()) {
            SimplifiedIntervalList availability = TableUtils.logicalTableAvailability(getTable());
            DateTime adjustedNow = new DateTime();
            if (! availability.isEmpty()) {
                DateTime firstUnavailable =  availability.getLast().getEnd();
                if (firstUnavailable.isBeforeNow()) {
                    adjustedNow = firstUnavailable;
                }
            }
            result = generateIntervals(adjustedNow, intervalsName, granularity, dateTimeFormatter);
        } else {
            result = generateIntervals(intervalsName, granularity, dateTimeFormatter);
        }
        return result;
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
    )
            throws BadApiRequestException {
        ApiRequestValidators.INSTANCE.validateTimeAlignment(granularity, intervals);
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
        return DefaultSortColumnGenerators.INSTANCE.generateSortColumns(sorts);
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
    protected LinkedHashSet<OrderByColumn> bindSorts(
            Map<String, SortDirection> sortDirectionMap,
            Set<LogicalMetric> logicalMetrics,
            MetricDictionary metricDictionary
    ) throws BadApiRequestException {
        return DefaultSortColumnGenerators.INSTANCE.generateSortColumns(sortDirectionMap, logicalMetrics, metricDictionary);
    }

    /**
     * Method to generate DateTime sort column from the map of columns and its direction.
     *
     * @param sortColumns  LinkedHashMap of columns and its direction. Using LinkedHashMap to preserve the order
     *
     * @return Instance of OrderByColumn for dateTime
     */
    protected Optional<OrderByColumn> bindDateTimeSortColumn(LinkedHashMap<String, SortDirection> sortColumns) {
        return DefaultSortColumnGenerators.INSTANCE.generateDateTimeSortColumn(sortColumns);
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
    protected Integer bindCount(String countRequest) {
        return generateInteger(countRequest, "count");
    }

    /**
     * Confirm count size is non negative.
     *
     * @param countRequest The value of the count from the request (if any)
     * @param count The bound value for the count
     */
    protected void validateCount(String countRequest, int count) {
        // This is the validation part for count that is inlined here because currently it is very brief.
        if (count < 0) {
            LOG.debug(INTEGER_INVALID.logFormat(countRequest, "count"));
            throw new BadApiRequestException(INTEGER_INVALID.logFormat(countRequest, "count"));
        }
    }

    /**
     * Bind the top N bucket size (if any).
     *
     * @param topNRequest The value of the topN bucket size from the request (if any)
     *
     * @return The requested value, zero if null or empty
     */
    protected int bindTopN(String topNRequest) {
        return generateInteger(topNRequest, "topN");
    }

    /**
     * Confirm the top N bucket size (if any) is valid.
     *
     * @param topNRequest The value of the count from the request (if any)
     * @param sorts collection of sorted columns
     * @param topN The bound value for the count
     */
    protected void validateTopN(String topNRequest,  int topN, LinkedHashSet<OrderByColumn> sorts) {
        // This is the validation part for topN that is inlined here because currently it is very brief.
        if (topN < 0) {
            LOG.debug(INTEGER_INVALID.logFormat(topNRequest, "topN"));
            throw new BadApiRequestException(INTEGER_INVALID.logFormat(topNRequest, "topN"));
        } else if (topN > 0 && this.sorts.isEmpty()) {
            LOG.debug(TOP_N_UNSORTED.logFormat(topNRequest));
            throw new BadApiRequestException(TOP_N_UNSORTED.format(topNRequest));
        }
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
        // TODO pick one!!
        ApiRequestValidators.INSTANCE.validateAggregatability(apiDimensions, apiFilters);
        DefaultDimensionGenerators.INSTANCE.validateAggregatability(apiDimensions, apiFilters);
    }

    // Binders and Validators complete

    /**
     * Method to remove the dateTime column from map of columns and its direction.
     *
     * @param sortColumns  map of columns and its direction
     *
     * @return  Map of columns and its direction without dateTime sort column
     */
    protected Map<String, SortDirection> removeDateTimeSortColumn(Map<String, SortDirection> sortColumns) {
        if (sortColumns != null && sortColumns.containsKey(DATE_TIME_COLUMN_NAME)) {
            sortColumns.remove(DATE_TIME_COLUMN_NAME);
            return sortColumns;
        } else {
            return sortColumns;
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

    //<-- End of validation and binding -->

    // Getters
    @Override
    public LogicalTable getTable() {
        return table;
    }

    @Override
    public Granularity getGranularity() {
        return granularity;
    }

    @Override
    public LinkedHashSet<Dimension> getDimensions() {
        return this.groupingDimensions;
    }

    @Override
    public LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> getDimensionFields() {
        return perDimensionFields;
    }

    @Override
    public LinkedHashSet<LogicalMetric> getLogicalMetrics() {
        return logicalMetrics;
    }

    @Override
    public List<Interval> getIntervals() {
        return intervals;
    }

    @Override
    public ApiFilters getApiFilters() {
        return apiFilters;
    }

    @Override
    public Map<LogicalMetric, Set<ApiHaving>> getHavings() {
        return havings;
    }

    @Override
    public LinkedHashSet<OrderByColumn> getSorts() {
        return sorts;
    }

    @Override
    public Optional<OrderByColumn> getDateTimeSort() {
        return Optional.ofNullable(dateTimeSort);
    }

    @Override
    public DateTimeZone getTimeZone() {
        return timeZone;
    }

    @Override
    @Deprecated
    public Having getQueryHaving() {
        return DefaultDruidHavingBuilder.INSTANCE.buildHavings(havings);
    }

    @Override
    public OptionalInt getTopN() {
        return topN == 0 ? OptionalInt.empty() : OptionalInt.of(topN);
    }

    @Override
    public OptionalInt getCount() {
        return count == 0 ? OptionalInt.empty() : OptionalInt.of(count);
    }

    @Override
    @Deprecated
    public Filter getQueryFilter() {
        try (TimedPhase timer = RequestLog.startTiming("BuildingDruidFilter")) {
            return getFilterBuilder().buildFilters(this.apiFilters);
        } catch (FilterBuilderException e) {
            LOG.debug(e.getMessage());
            throw new BadApiRequestException(e);
        }
    }

    @Override
    @Deprecated
    public DruidFilterBuilder getFilterBuilder() {
        return new DruidOrFilterBuilder();
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
        return FilterBinders.INSTANCE::generateFilters;
    }

    // CHECKSTYLE:OFF

    @Override
    public DataApiRequestImpl withTable(LogicalTable table) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    @Override
    public DataApiRequestImpl withGranularity(Granularity granularity) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    @Override
    public DataApiRequestImpl withDimensions(LinkedHashSet<Dimension> dimensions) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    @Override
    public DataApiRequestImpl withPerDimensionFields(LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    @Override
    public DataApiRequestImpl withLogicalMetrics(LinkedHashSet<LogicalMetric> logicalMetrics) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    @Override
    public DataApiRequestImpl withIntervals(List<Interval> intervals) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    @Deprecated
    public DataApiRequestImpl withIntervals(Set<Interval> intervals) {
        return withIntervals(new ArrayList<>(intervals));
    }

    @Override
    public DataApiRequestImpl withFilters(ApiFilters apiFilters) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    @Override
    public DataApiRequestImpl withHavings(Map<LogicalMetric, Set<ApiHaving>> havings) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    @Override
    public DataApiRequestImpl withSorts(LinkedHashSet<OrderByColumn> sorts) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    @Override
    public DataApiRequestImpl withTimeSort(Optional<OrderByColumn> timeSort) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    public DataApiRequestImpl withTimeZone(DateTimeZone timeZone) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    // Result Set Truncations

    @Override
    public DataApiRequestImpl withCount(int count) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    @Override
    public DataApiRequestImpl withTopN(int topN) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    @Override
    public DataApiRequest withPaginationParameters(PaginationParameters paginationParameters) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    public DataApiRequest withFormat(ResponseFormatType format) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    @Override
    public DataApiRequest withDownloadFilename(String downloadFilename) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }

    @Override
    public DataApiRequestImpl withAsyncAfter(long asyncAfter) {
        return new DataApiRequestImpl(table, granularity, groupingDimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, Optional.ofNullable(dateTimeSort), timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter);
    }


    // Super deprecated stuff below
    @Override
    public DataApiRequestImpl withBuilder(Response.ResponseBuilder builder) {
        return this;
    }

    @Override
    public DataApiRequestImpl withFilterBuilder(DruidFilterBuilder filterBuilder) {
        return this;
    }
    // CHECKSTYLE:ON
}
