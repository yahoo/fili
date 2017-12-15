// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.ApiHaving;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.filters.ApiFilters;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import javax.ws.rs.core.Response;

/**
 * A value object style DataApiRequestImpl with maplike storage for extensible construction.
 */
public class DataApiRequestVauleObjectImpl implements DataApiRequest {

    protected static  String TABLE_KEY = "table";
    protected static  String GRANULARITY_KEY = "table";
    protected static  String GROUPING_DIMENSIONS_KEY = "groupingDimensions";
    protected static  String GROUPING_DIMENSION_FIELDS_KEY = "dimensionFields";
    protected static  String LOGICAL_METRICS_KEY = "logicalMetrics";

    protected static  String INTERVALS_KEY = "intervals";
    protected static  String API_FILTERS_KEY = "apiFilters";
    protected static  String HAVINGS_KEY = "havings";
    protected static  String SORTS_KEY = "sorts";

    protected static  String TIME_SORT_KEY = "dateTimeSort";
    protected static  String TIME_ZONE_KEY = "timeZone";
    protected static  String TOP_N_KEY = "topN";
    protected static  String COUNT_KEY = "count";

    protected static String PAGINATION_PARAMETERS_KEY = "paginationParameters";
    protected static String FORMAT_KEY = "format";
    protected static String ASYNC_AFTER_KEY = "asyncAfter";


    protected  Map<String, Object> values;

    /**
     * Constructor.
     *
     * Build a DataApiRequest with no fields configured.
     */
    public DataApiRequestVauleObjectImpl() {
        values = new LinkedHashMap<>();
    }

    /**
     * Constructor.
     *
     * Build a DataApiRequest with an initialized set of value.
     *
     * @param values  The map of values to copy.
     */
    protected DataApiRequestVauleObjectImpl(Map<String, Object> values) {
        this.values = new LinkedHashMap<>(values);
    }

    /**
     * Constructor.
     *
     * Build a copy of an existing data api request value object with a single field updated.
     *
     * @param apiRequest  The instance to copy
     * @param key  The field name of the field to be altered.
     * @param changeValue The new value for this field.
     */
    public DataApiRequestVauleObjectImpl(DataApiRequestVauleObjectImpl apiRequest, String key, Object changeValue) {
        this.values = new LinkedHashMap<>(apiRequest.values);
        values.put(key, changeValue);
    }

    @Override
    public LogicalTable getTable() {
        return (LogicalTable) values.get(TABLE_KEY);
    }

    @Override
    public Granularity getGranularity() {
        return (Granularity) values.get(GRANULARITY_KEY);
    }

    @Override
    public Set<Dimension> getDimensions() {
        return (Set<Dimension>) values.get(GROUPING_DIMENSIONS_KEY);
    }

    @Override
    public LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> getDimensionFields() {
        return (LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>) values.get(GROUPING_DIMENSION_FIELDS_KEY);
    }

    @Override
    public Set<LogicalMetric> getLogicalMetrics() {
        return (Set<LogicalMetric>) values.get(LOGICAL_METRICS_KEY);
    }

    @Override
    public List<Interval> getIntervals() {
        return (List<Interval>) values.get(INTERVALS_KEY);
    }

    @Override
    public ApiFilters getApiFilters() {
        return (ApiFilters) values.get(API_FILTERS_KEY);
    }

    @Override
    public Map<LogicalMetric, Set<ApiHaving>> getHavings() {
        return (Map<LogicalMetric, Set<ApiHaving>>) values.get(HAVINGS_KEY);
    }

    @Override
    public LinkedHashSet<OrderByColumn> getSorts() {
        return (LinkedHashSet<OrderByColumn>) values.get(SORTS_KEY);
    }

    @Override
    public Optional<OrderByColumn> getDateTimeSort() {
        return Optional.ofNullable((OrderByColumn) values.get(TIME_SORT_KEY));
    }

    @Override
    public DateTimeZone getTimeZone() {
        return (DateTimeZone) values.get(TIME_ZONE_KEY);
    }

    @Override
    public OptionalInt getTopN() {
        return Optional.ofNullable((Integer) values.get(TOP_N_KEY)).map(OptionalInt::of).orElseGet(OptionalInt::empty);
    }

    @Override
    public OptionalInt getCount() {
        return Optional.ofNullable((Integer) values.get(COUNT_KEY)).map(OptionalInt::of).orElseGet(OptionalInt::empty);
    }

    @Override
    @Deprecated
    public Filter getQueryFilter() {
        throw new UnsupportedOperationException("Query Filter should be built outside ApiRequest");
    }

    @Override
    @Deprecated
    public Having getQueryHaving() {
        throw new UnsupportedOperationException("Query having should be built outside ApiRequest");
    }

    @Override
    @Deprecated
    public Map<Dimension, Set<ApiFilter>> generateFilters(
             String filterQuery,  LogicalTable table,  DimensionDictionary dimensionDictionary
    ) {
        throw new UnsupportedOperationException("Query Filter generation should be built outside ApiRequest");
    }

    @Override
    public ResponseFormatType getFormat() {
        return (ResponseFormatType) values.get(FORMAT_KEY);
    }

    @Override
    public Optional<PaginationParameters> getPaginationParameters() {
        return Optional.ofNullable(
                (PaginationParameters) values.get(PAGINATION_PARAMETERS_KEY)
        );
    }

    @Override
    public Long getAsyncAfter() {
        return (Long) values.get(ASYNC_AFTER_KEY);
    }

    @Override
    @Deprecated
    public DruidFilterBuilder getFilterBuilder() {
        throw new UnsupportedOperationException("Query Filter should be built outside ApiRequest");
    }

    /**
     * Make a copy of this instance with one field and value updated.
     *
     * @param field  The field name to be updated.
     * @param value  The field value to be updated.
     *
     * @return A modified copy of this DataApiRequest
     */
    protected DataApiRequestVauleObjectImpl withField(String field, Object value) {
        return new DataApiRequestVauleObjectImpl(this, field, value);
    }

    @Override
    public DataApiRequestVauleObjectImpl withTable(LogicalTable table) {
        return withField(TABLE_KEY, table);
    }

    @Override
    public DataApiRequestVauleObjectImpl withGranularity(Granularity granularity) {
        return withField(GRANULARITY_KEY, granularity);
    }

    @Override
    public DataApiRequestVauleObjectImpl withDimensions(LinkedHashSet<Dimension> dimensions) {
        return withField(GROUPING_DIMENSIONS_KEY, dimensions);
    }

    @Override
    public DataApiRequestVauleObjectImpl withPerDimensionFields(
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields
    ) {
        return withField(GROUPING_DIMENSION_FIELDS_KEY, perDimensionFields);
    }

    @Override
    public DataApiRequestVauleObjectImpl withLogicalMetrics(LinkedHashSet<LogicalMetric> logicalMetrics) {
        return withField(LOGICAL_METRICS_KEY, logicalMetrics);
    }

    @Override
    public DataApiRequestVauleObjectImpl withIntervals(List<Interval> intervals) {
        return withField(INTERVALS_KEY, intervals);
    }

    @Override
    @Deprecated
    public DataApiRequestVauleObjectImpl withIntervals(Set<Interval> intervals) {
        return withIntervals(new ArrayList<>(intervals));
    }

    @Override
    public DataApiRequestVauleObjectImpl withFilters(ApiFilters filters) {
        return withField(API_FILTERS_KEY, filters);
    }

    @Override
    public DataApiRequestVauleObjectImpl withHavings(Map<LogicalMetric, Set<ApiHaving>> havings) {
        return withField(HAVINGS_KEY, havings);
    }

    @Override
    public DataApiRequestVauleObjectImpl withSorts(LinkedHashSet<OrderByColumn> sorts) {
        return withField(SORTS_KEY, sorts);
    }

    @Override
    public DataApiRequestVauleObjectImpl withTimeSort(Optional<OrderByColumn> timeSort) {
        return withField(TIME_SORT_KEY, timeSort);
    }

    @Override
    public DataApiRequestVauleObjectImpl withTimeZone(DateTimeZone timeZone) {
        return withField(TIME_ZONE_KEY, timeZone);
    }

    @Override
    public DataApiRequestVauleObjectImpl withTopN(int topN) {
        return withField(TOP_N_KEY, topN);
    }

    @Override
    public DataApiRequestVauleObjectImpl withCount(int count) {
        return withField(COUNT_KEY, count);
    }

    @Override
    public DataApiRequestVauleObjectImpl withPaginationParameters(Optional<PaginationParameters> paginationParameters) {
        return withField(PAGINATION_PARAMETERS_KEY, paginationParameters);
    }

    @Override
    public DataApiRequestVauleObjectImpl withFormat(ResponseFormatType format) {
        return withField(FORMAT_KEY, format);
    }

    @Override
    public DataApiRequestVauleObjectImpl withAsyncAfter(long asyncAfter) {
        return withField(ASYNC_AFTER_KEY, asyncAfter);
    }

    @Override
    @Deprecated
    public DataApiRequest withBuilder(Response.ResponseBuilder builder) {
        throw new UnsupportedOperationException("Response should be built outside ApiRequest");
    }

    @Override
    @Deprecated
    public DataApiRequest withFilterBuilder(DruidFilterBuilder filterBuilder) {
        throw new UnsupportedOperationException("Druid filters should be built outside ApiRequest");
    }
}
