package com.yahoo.bard.webservice.web.apirequest.rfc;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.util.UnmodifiableLinkedHashMap;
import com.yahoo.bard.webservice.util.UnmodifiableLinkedHashSet;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.ApiHaving;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.filters.ApiFilters;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

public class DataApiRequestImpl implements DataApiRequest {

    private final LogicalTable table;
    private final Granularity granularity;
    private final LinkedHashSet<Dimension> dimensions;
    private final LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields;
    private final LinkedHashSet<LogicalMetric> metrics;
    private final List<Interval> intervals;
    private final ApiFilters apiFilters;
    private final LinkedHashMap<LogicalMetric, Set<ApiHaving>> havings;
    private final OrderByColumn dateTimeSort;
    private final LinkedHashSet<OrderByColumn> sorts;
    private final Integer count;
    private final Integer topN;
    private final ResponseFormatType format;
    private final String downloadFilename;
    private final DateTimeZone timeZone;
    private final Long asyncAfter;
    private final PaginationParameters paginationParameters;

    public DataApiRequestImpl(
            LogicalTable table,
            Granularity granularity,
            LinkedHashSet<Dimension> dimensions,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields,
            LinkedHashSet<LogicalMetric> metrics,
            List<Interval> intervals,
            ApiFilters apiFilters,
            LinkedHashMap<LogicalMetric, Set<ApiHaving>> havings,
            LinkedHashSet<OrderByColumn> sorts,
            int count,
            int topN,
            ResponseFormatType format,
            String downloadFilename,
            DateTimeZone timeZone,
            Long asyncAfter,
            PaginationParameters paginationParameters
    ) {
        this.table = table;
        this.granularity = granularity;
        this.dimensions = UnmodifiableLinkedHashSet.of(dimensions);
        this.perDimensionFields = UnmodifiableLinkedHashMap.of(perDimensionFields);
        this.metrics = UnmodifiableLinkedHashSet.of(metrics);
        this.intervals = Collections.unmodifiableList(new ArrayList<>(intervals));
        // TODO should we make an immutable version of ApiFilters?
        this.apiFilters = new ApiFilters(apiFilters);
        this.havings = UnmodifiableLinkedHashMap.of(havings);
        this.dateTimeSort = extractDateTimeSort(sorts);
        this.sorts = UnmodifiableLinkedHashSet.of(extractStandardSorts(sorts));
        this.count = count;
        this.topN = topN;
        this.format = format;
        this.downloadFilename = downloadFilename;
        this.timeZone = timeZone;
        this.asyncAfter = asyncAfter;
        this.paginationParameters = paginationParameters;
    }

    protected static OrderByColumn extractDateTimeSort(LinkedHashSet<OrderByColumn> sorts) {
        return sorts.stream()
                .filter(orderBy -> orderBy.getDimension().equalsIgnoreCase(DataApiRequest.DATE_TIME_STRING))
                .findFirst()
                .orElse(null);
    }

    protected static LinkedHashSet<OrderByColumn> extractStandardSorts(LinkedHashSet<OrderByColumn> sorts) {
        return sorts.stream()
                .filter(orderBy -> !orderBy.getDimension().equalsIgnoreCase(DataApiRequest.DATE_TIME_STRING))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public LogicalTable getTable() {
        return table;
    }

    @Override
    public Granularity getGranularity() {
        return granularity;
    }

    @Override
    public Set<Dimension> getDimensions() {
        return dimensions;
    }

    public LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> getDimensionFields() {
        return perDimensionFields;
    }

    @Override
    public Set<LogicalMetric> getLogicalMetrics() {
        return metrics;
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
    public LinkedHashMap<LogicalMetric, Set<ApiHaving>> getHavings() {
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
    public OptionalInt getCount() {
        return OptionalInt.of(count);
    }


    @Override
    public OptionalInt getTopN() {
        return OptionalInt.of(topN);
    }

    @Override
    public ResponseFormatType getFormat() {
        return format;
    }

    @Override
    public Optional<String> getDownloadFilename() {
        return Optional.ofNullable(downloadFilename);
    }

    @Override
    public DateTimeZone getTimeZone() {
        return timeZone;
    }

    @Override
    public Long getAsyncAfter() {
        return asyncAfter;
    }

    @Override
    public Optional<PaginationParameters> getPaginationParameters() {
        return Optional.ofNullable(paginationParameters);
    }

    // TODO implement withers
    // UNORDERED

    @Override
    public Filter getQueryFilter() {
        return null;
    }

    @Override
    public Having getQueryHaving() {
        return null;
    }

    @Override
    public DruidFilterBuilder getFilterBuilder() {
        return null;
    }

    @Override
    public Map<Dimension, Set<ApiFilter>> generateFilters(
            final String filterQuery, final LogicalTable table, final DimensionDictionary dimensionDictionary
    ) {
        return null;
    }

    @Override
    public DataApiRequest withTable(final LogicalTable table) {
        return new DataApiRequestImpl(getTimeZone(), ...)
    }

    @Override
    public DataApiRequest withGranularity(final Granularity granularity) {
        return null;
    }

    @Override
    public DataApiRequest withDimensions(final LinkedHashSet<Dimension> dimensions) {
        return null;
    }

    @Override
    public DataApiRequest withPerDimensionFields(final LinkedHashMap<Dimension, LinkedHashSet<DimensionField>>
            perDimensionFields) {
        return null;
    }

    @Override
    public DataApiRequest withLogicalMetrics(final LinkedHashSet<LogicalMetric> logicalMetrics) {
        return null;
    }

    @Override
    public DataApiRequest withIntervals(final List<Interval> intervals) {
        return null;
    }

    @Override
    public DataApiRequest withIntervals(final Set<Interval> intervals) {
        return null;
    }

    @Override
    public DataApiRequest withFilters(final ApiFilters filters) {
        return null;
    }

    @Override
    public DataApiRequest withHavings(LinkedHashMap<LogicalMetric, Set<ApiHaving>> havings) {
        return null;
    }

    @Override
    public DataApiRequest withSorts(final LinkedHashSet<OrderByColumn> sorts) {
        return null;
    }

    @Override
    public DataApiRequest withTimeSort(final Optional<OrderByColumn> timeSort) {
        return null;
    }

    @Override
    public DataApiRequest withTimeZone(final DateTimeZone timeZone) {
        return null;
    }

    @Override
    public DataApiRequest withTopN(final int topN) {
        return null;
    }

    @Override
    public DataApiRequest withCount(final int count) {
        return null;
    }

    @Override
    public DataApiRequest withPaginationParameters(final Optional<PaginationParameters> paginationParameters) {
        return null;
    }

    @Override
    public DataApiRequest withFormat(final ResponseFormatType format) {
        return null;
    }

    @Override
    public DataApiRequest withDownloadFilename(final String downloadFilename) {
        return null;
    }

    @Override
    public DataApiRequest withAsyncAfter(final long asyncAfter) {
        return null;
    }

    @Override
    public DataApiRequest withBuilder(final Response.ResponseBuilder builder) {
        return null;
    }

    @Override
    public DataApiRequest withFilterBuilder(final DruidFilterBuilder filterBuilder) {
        return null;
    }
}
