// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.ApiHaving;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.requestParameters.RequestColumn;
import com.yahoo.bard.webservice.web.filters.ApiFilters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.UnmodifiableMultiValuedMap;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ProtocolMetricDataApiRequest supports the parameterized metric contracts used by protocol metrics.
 */
public class ExtensibleDataApiRequestImpl extends DataApiRequestImpl {

    public static final String PSEUDO_DIMENSION_KEY = "psedoDimension";

    protected final MultiValuedMap<String, String> queryParameters;
    protected final Map<String, Object> extendedObjects;

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
     * @param queryParameters  A multimap of parameterized values
     * @param extendedObjects A map of other request or bound objects for extended features
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
    public ExtensibleDataApiRequestImpl(
            String tableName,
            String granularity,
            List<RequestColumn> dimensions,
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
            String perPage,
            String page,
            MultiValuedMap<String, String> queryParameters,
            Map<String, Object> extendedObjects,
            BardConfigResources bardConfigResources
    ) throws BadApiRequestException {
        super(
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
                bardConfigResources
        );
        this.queryParameters = UnmodifiableMultiValuedMap.unmodifiableMultiValuedMap(queryParameters);
        this.extendedObjects = Collections.unmodifiableMap(extendedObjects);
    }

    /**
     * Constructor with all bindables bound, meant to be used for rewriting apiRequest.
     *
     * Filter builder in constructor should be removed when some deprecations come out.
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
     * @param paginationParameters  Pagination info
     * @param format  Format for the response
     * @param downloadFilename  The filename for the response to be downloaded as. If null indicates response should
     * not be downloaded.
     * @param asyncAfter  How long in milliseconds the user is willing to wait for a synchronous response
     * @param optimizable  Whether or not this request can be safely optimized into a topN or timeseries Druid query,
     * if this is false a groupBy should always be built, even if the request would otherwise be eligible for one of
     * @param queryParameters Additional parameters from the request
     * @param extendedObjects A map of other request or bound objects for extended features
     */
    protected ExtensibleDataApiRequestImpl(
            LogicalTable table,
            Granularity granularity,
            LinkedHashSet<Dimension> groupingDimensions,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields,
            LinkedHashSet<LogicalMetric> logicalMetrics,
            List<Interval> intervals,
            ApiFilters apiFilters,
            Map<LogicalMetric, Set<ApiHaving>> havings,
            LinkedHashSet<OrderByColumn> sorts,
            OrderByColumn dateTimeSort,
            DateTimeZone timeZone,
            Integer topN,
            Integer count,
            PaginationParameters paginationParameters,
            ResponseFormatType format,
            String downloadFilename,
            Long asyncAfter,
            boolean optimizable,
            MultiValuedMap<String, String> queryParameters,
            Map<String, Object> extendedObjects
    ) {
        super(
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
                downloadFilename,
                asyncAfter,
                optimizable
        );
        this.queryParameters = UnmodifiableMultiValuedMap.unmodifiableMultiValuedMap(queryParameters);
        this.extendedObjects = Collections.unmodifiableMap(extendedObjects);
        // Metric generator shouldn't be required if objects are already bound.
        this.metricBinder = null;
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
        metricBinder.validateMetrics(metrics, logicalTable);
    }

    public MultiValuedMap<String, String> getQueryParameters() {
        return queryParameters;
    }

    public Map<String, Object> getExtendedObjects() {
        return extendedObjects;
    }

    // CHECKSTYLE:OFF
    @Override
    public ExtensibleDataApiRequestImpl withFormat(ResponseFormatType format) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withDownloadFilename(String downloadFilename) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withPaginationParameters(PaginationParameters paginationParameters) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withTable(LogicalTable table) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withGranularity(Granularity granularity) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withDimensions(LinkedHashSet<Dimension> dimensions) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withPerDimensionFields(LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withLogicalMetrics(LinkedHashSet<LogicalMetric> logicalMetrics) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withIntervals(List<Interval> intervals) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withFilters(ApiFilters apiFilters) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withHavings(LinkedHashMap<LogicalMetric, Set<ApiHaving>> havings) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withSorts(LinkedHashSet<OrderByColumn> sorts) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withTimeSort(OrderByColumn dateTimeSort) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withTimeZone(DateTimeZone timeZone) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withTopN(Integer topN) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withCount(Integer count) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    @Override
    public ExtensibleDataApiRequestImpl withAsyncAfter(long asyncAfter) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    public ExtensibleDataApiRequestImpl withDruidOptimizations(boolean optimizable) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    public ExtensibleDataApiRequestImpl withQueryParameters(MultiValuedMap<String, String> queryParameters) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }

    public ExtensibleDataApiRequestImpl withExtendedObjects(Map<String, Object> extendedObjects) {
        return new ExtensibleDataApiRequestImpl(table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, dateTimeSort, timeZone, topN, count, paginationParameters, format, downloadFilename, asyncAfter, optimizable, queryParameters, extendedObjects);
    }
    // CHECKSTYLE:ON
}
