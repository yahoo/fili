// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.FilterBuilderException;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.builders.DefaultDruidHavingBuilder;
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.ApiHaving;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.apirequest.binders.HavingGenerator;
import com.yahoo.bard.webservice.web.filters.ApiFilters;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import javax.ws.rs.core.Response;

/**
 * Data API Request Implementation binds, validates, and models the parts of a request to the data endpoint.
 */
public class DataApiRequestPojoImpl implements DataApiRequest {

    protected final ResponseFormatType format;
    protected final long asyncAfter;
    protected final PaginationParameters paginationParameters;


    private static final Logger LOG = LoggerFactory.getLogger(DataApiRequestPojoImpl.class);
    private final LogicalTable table;

    private final Granularity granularity;

    private final Set<Dimension> dimensions;
    private final LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields;
    private final Set<LogicalMetric> logicalMetrics;
    private final List<Interval> intervals;
    private final ApiFilters apiFilters;
    private final Map<LogicalMetric, Set<ApiHaving>> havings;
    private final LinkedHashSet<OrderByColumn> sorts;
    private final int count;
    private final int topN;

    private final DateTimeZone timeZone;

    private final HavingGenerator havingApiGenerator;

    private final Optional<OrderByColumn> dateTimeSort;

    private final DruidFilterBuilder druidFilterBuilder;

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
     * @param havingApiGenerator  A generator to generate havings map for the request
     * @param dateTimeSort  A dateTime sort column with its direction
     * @param filterBuilder A factory object for druid filters
     */
    public DataApiRequestPojoImpl(
            ResponseFormatType format,
            PaginationParameters paginationParameters,
            LogicalTable table,
            Granularity granularity,
            Set<Dimension> dimensions,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields,
            Set<LogicalMetric> logicalMetrics,
            List<Interval> intervals,
            ApiFilters apiFilters,
            Map<LogicalMetric, Set<ApiHaving>> havings,
            LinkedHashSet<OrderByColumn> sorts,
            int count,
            int topN,
            long asyncAfter,
            DateTimeZone timeZone,
            HavingGenerator havingApiGenerator,
            Optional<OrderByColumn> dateTimeSort,
            DruidFilterBuilder filterBuilder
    ) {
        this.format = format;
        this.asyncAfter = asyncAfter;
        this.paginationParameters = paginationParameters;

        //super(format, asyncAfter, paginationParameters, uriInfo);
        this.table = table;
        this.granularity = granularity;
        this.dimensions = dimensions;
        this.perDimensionFields = perDimensionFields;
        this.logicalMetrics = logicalMetrics;
        this.intervals = intervals;
        this.apiFilters = apiFilters;
        this.havings = havings;
        this.sorts = sorts;
        this.count = count;
        this.topN = topN;
        this.timeZone = timeZone;
        this.havingApiGenerator = havingApiGenerator;
        this.dateTimeSort = dateTimeSort;
        this.druidFilterBuilder = filterBuilder;

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
        return DefaultLogicalMetricsGenerators.INSTANCE.generateLogicalMetrics(
                apiMetricQuery,
                metricDictionary,
                dimensionDictionary,
                table
        );
    }

    /**
     * Gets the filter dimensions form the given set of filter objects.
     *
     * @return Set of filter dimensions.
     */
    @Override
    public Set<Dimension> getFilterDimensions() {
        return apiFilters.keySet();
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
    public Map<Dimension, Set<ApiFilter>> generateFilters(
            String filterQuery, LogicalTable table, DimensionDictionary dimensionDictionary
    ) {
        return DefaultFilterGenerator.generateFilters(filterQuery, table, dimensionDictionary);
    }


    @Override
    public Map<LogicalMetric, Set<ApiHaving>> getHavings() {
        return this.havings;
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
    public Having getQueryHaving() {
        return DefaultDruidHavingBuilder.INSTANCE.buildHavings(havings);
    }


    @Override
    public LinkedHashSet<OrderByColumn> getSorts() {
        return this.sorts;
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

    @Override
    public Optional<OrderByColumn> getDateTimeSort() {
        return dateTimeSort;
    }

    @Override
    public Long getAsyncAfter() {
        return asyncAfter;
    }

    @Override
    public DruidFilterBuilder getFilterBuilder() {
        return druidFilterBuilder;
    }

    @Override
    public ResponseFormatType getFormat() {
        return format;
    }

    @Override
    public Optional<PaginationParameters> getPaginationParameters() {
        return Optional.ofNullable(paginationParameters);
    }


    // CHECKSTYLE:OFF
    @Override
    public DataApiRequestPojoImpl withTable(LogicalTable table) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }

    @Override
    public DataApiRequestPojoImpl withGranularity(Granularity granularity) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }

    @Override
    public DataApiRequestPojoImpl withDimensions(LinkedHashSet<Dimension> dimensions) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }


    @Override
    public DataApiRequestPojoImpl withPerDimensionFields(LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }

    @Override
    public DataApiRequestPojoImpl withLogicalMetrics(LinkedHashSet<LogicalMetric> logicalMetrics) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }

    @Override
    public DataApiRequestPojoImpl withIntervals(Set<Interval> intervals) {
        return withIntervals(new SimplifiedIntervalList(intervals));
    }

    @Override
    public DataApiRequestPojoImpl withIntervals(List<Interval> intervals) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }

    @Override
    public DataApiRequestPojoImpl withFilters(ApiFilters apiFilters) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }

    @Override
    public DataApiRequest withHavings(Map<LogicalMetric, Set<ApiHaving>> havings) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }


    @Override
    public DataApiRequestPojoImpl withSorts(LinkedHashSet<OrderByColumn> sorts) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }

    @Override
    public DataApiRequest withTimeSort(Optional<OrderByColumn> timeSort) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, timeSort, druidFilterBuilder);
    }

    @Override
    public DataApiRequest withTimeZone(DateTimeZone timeZone) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }

    @Override
    public DataApiRequest withTopN(int topN) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }

    @Override
    public DataApiRequest withCount(int count) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }

    @Override
    public DataApiRequestPojoImpl withPaginationParameters(PaginationParameters paginationParameters) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }

    @Override
    public DataApiRequestPojoImpl withFormat(ResponseFormatType format) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }

    @Override
    public DataApiRequest withDownloadFilename(String downloadFilename) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }

    @Override
    public DataApiRequest withAsyncAfter(final long asyncAfter) {
        return new DataApiRequestPojoImpl(format, paginationParameters, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, druidFilterBuilder);
    }

    // Super deprecated stuff below
    @Override
    public DataApiRequest withBuilder(Response.ResponseBuilder builder) {
        return this;
    }

    @Override
    public DataApiRequest withFilterBuilder(DruidFilterBuilder filterBuilder) {
        return this;
    }

    // CHECKSTYLE:ON


}
