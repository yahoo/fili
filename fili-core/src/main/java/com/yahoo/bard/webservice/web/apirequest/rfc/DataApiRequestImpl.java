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
import com.yahoo.bard.webservice.web.filters.UnmodifiableApiFilters;
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
    private final Optional<OrderByColumn> dateTimeSort;
    private final LinkedHashSet<OrderByColumn> standardSorts;
    private final LinkedHashSet<OrderByColumn> allSorts;
    private final Optional<Integer> count;
    private final Optional<Integer> topN;
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
            LinkedHashSet<OrderByColumn> allSorts,
            Integer count,
            Integer topN,
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
        this.apiFilters = UnmodifiableApiFilters.of(new ApiFilters(apiFilters));
        this.havings = UnmodifiableLinkedHashMap.of(havings);
        this.dateTimeSort = extractDateTimeSort(allSorts);
        this.standardSorts = UnmodifiableLinkedHashSet.of(extractStandardSorts(allSorts));
        this.allSorts = UnmodifiableLinkedHashSet.of(allSorts);
        this.count = Optional.ofNullable(count);
        this.topN = Optional.ofNullable(topN);
        this.format = format;
        this.downloadFilename = downloadFilename;
        this.timeZone = timeZone;
        this.asyncAfter = asyncAfter;
        this.paginationParameters = paginationParameters;
    }

    /**
     * Constructor for handling a date time sort and standard allSorts separately. The public constructor assumes they
     * are together in the {@code allSorts} parameter. This is simply a convenience constructor that is made available to
     * subclasses. This is mostly intended to back the {@code withTimeSort(Optional<OrderByColumn>)} implementation.
     *
     * @param table
     * @param granularity
     * @param dimensions
     * @param perDimensionFields
     * @param metrics
     * @param intervals
     * @param apiFilters
     * @param havings
     * @param standardSorts sorts WITHOUT datetime sort.
     * @param count
     * @param topN
     * @param format
     * @param downloadFilename
     * @param timeZone
     * @param asyncAfter
     * @param paginationParameters
     */
    protected DataApiRequestImpl(
            LogicalTable table,
            Granularity granularity,
            LinkedHashSet<Dimension> dimensions,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields,
            LinkedHashSet<LogicalMetric> metrics,
            List<Interval> intervals,
            ApiFilters apiFilters,
            LinkedHashMap<LogicalMetric, Set<ApiHaving>> havings,
            Optional<OrderByColumn> dateTimeSort,
            LinkedHashSet<OrderByColumn> standardSorts,
            Optional<Integer> count,
            Optional<Integer> topN,
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
        this.dateTimeSort = dateTimeSort;
        this.standardSorts = UnmodifiableLinkedHashSet.of(standardSorts);
        this.allSorts = UnmodifiableLinkedHashSet.of(combineSorts(dateTimeSort, standardSorts));
        this.count = count;
        this.topN = topN;
        this.format = format;
        this.downloadFilename = downloadFilename;
        this.timeZone = timeZone;
        this.asyncAfter = asyncAfter;
        this.paginationParameters = paginationParameters;

    }

    protected static Optional<OrderByColumn> extractDateTimeSort(LinkedHashSet<OrderByColumn> sorts) {
        return sorts.stream()
                .filter(orderBy -> orderBy.getDimension().equalsIgnoreCase(DataApiRequest.DATE_TIME_STRING))
                .findFirst();
    }

    protected static LinkedHashSet<OrderByColumn> extractStandardSorts(LinkedHashSet<OrderByColumn> sorts) {
        return sorts == null ?
                    null :
                    sorts.stream()
                            .filter(orderBy ->
                                    !orderBy.getDimension().equalsIgnoreCase(DataApiRequest.DATE_TIME_STRING))
                            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    protected static LinkedHashSet<OrderByColumn> combineSorts(
            Optional<OrderByColumn> dateTimeSort,
            LinkedHashSet<OrderByColumn> standardSorts
    ) {
        LinkedHashSet<OrderByColumn> result = new LinkedHashSet<>();
        dateTimeSort.ifPresent(result::add);
        result.addAll(standardSorts);
        return result;
    }

    // *******************************************
    // ************** STANDARD GETS **************
    // *******************************************

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
        return standardSorts;
    }

    @Override
    public Optional<OrderByColumn> getDateTimeSort() {
        return dateTimeSort;
    }

    @Override
    public LinkedHashSet<OrderByColumn> getAllSorts() {
        return allSorts;
    }

    @Override
    public Optional<Integer> getCount() {
        return count;
    }


    @Override
    public Optional<Integer> getTopN() {
        return topN;
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

    //*************************************
    //************** WITHERS **************
    //*************************************

    @Override
    public DataApiRequest withTable(LogicalTable table) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                allSorts,
                count.orElse(null),
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withGranularity(Granularity granularity) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                allSorts,
                count.orElse(null),
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withDimensions(LinkedHashSet<Dimension> dimensions) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                allSorts,
                count.orElse(null),
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withPerDimensionFields(
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields
    ) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                allSorts,
                count.orElse(null),
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withLogicalMetrics(LinkedHashSet<LogicalMetric> logicalMetrics) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                logicalMetrics,
                intervals,
                apiFilters,
                havings,
                allSorts,
                count.orElse(null),
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withIntervals(List<Interval> intervals) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                allSorts,
                count.orElse(null),
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withIntervals(Set<Interval> intervals) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals.stream().collect(Collectors.toList()),
                apiFilters,
                havings,
                allSorts,
                count.orElse(null),
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withFilters(ApiFilters filters) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                filters,
                havings,
                allSorts,
                count.orElse(null),
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withHavings(LinkedHashMap<LogicalMetric, Set<ApiHaving>> havings) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                allSorts,
                count.orElse(null),
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    /**
     * This CAN include a time sort.
     *
     * @param sorts
     * @return
     */
    @Override
    public DataApiRequest withSorts(LinkedHashSet<OrderByColumn> sorts) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                dateTimeSort,
                sorts,
                count,
                topN,
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withTimeSort(Optional<OrderByColumn> timeSort) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                timeSort,
                allSorts,
                count,
                topN,
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withTimeZone(DateTimeZone timeZone) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                allSorts,
                count.orElse(null),
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withTopN(int topN) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                allSorts,
                count.orElse(null),
                topN,
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withCount(int count) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                allSorts,
                count,
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withPaginationParameters(Optional<PaginationParameters> paginationParameters) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                allSorts,
                count.orElse(null),
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters.orElse(null)
        );
    }

    @Override
    public DataApiRequest withFormat(ResponseFormatType format) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                allSorts,
                count.orElse(null),
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withDownloadFilename(String downloadFilename) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                allSorts,
                count.orElse(null),
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    @Override
    public DataApiRequest withAsyncAfter(long asyncAfter) {
        return new DataApiRequestImpl(
                table,
                granularity,
                dimensions,
                perDimensionFields,
                metrics,
                intervals,
                apiFilters,
                havings,
                allSorts,
                count.orElse(null),
                topN.orElse(null),
                format,
                downloadFilename,
                timeZone,
                asyncAfter,
                paginationParameters
        );
    }

    //*************************************************************
    //************** DEPRECATED METHODS ON INTERFACE **************
    //*************************************************************

    /**
     * Unsupported operation. Throws UnsupportedOperationException if invoked. Druid filters are being split from the
     * ApiRequest model and should be handled separately.
     *
     * @throws UnsupportedOperationException if invoked.
     */
    @Deprecated
    @Override
    public Filter getQueryFilter() {
        throw new UnsupportedOperationException("Druid filters are being split from the ApiRequest model and " +
                "should be handled separately.");
    }

    /**
     * Unsupported operation. Throws UnsupportedOperationException if invoked. Druid havings are being split from the
     * ApiRequest model and should be handled separately.
     *
     * @throws UnsupportedOperationException if invoked.
     */
    @Deprecated
    @Override
    public Having getQueryHaving() {
        throw new UnsupportedOperationException("Druid havings are being split from the ApiRequest model and should " +
                "be handled separately.");
    }

    /**
     * Unsupported operation. Throws UnsupportedOperationException if invoked. Druid filters are being split from the
     * ApiRequest model and should be handled separately.
     *
     * @throws UnsupportedOperationException if invoked.
     */
    @Deprecated
    @Override
    public DruidFilterBuilder getFilterBuilder() {
        throw new UnsupportedOperationException("Druid filters are being split from the ApiRequest model and " +
                "should be handled separately.");
    }

    /**
     * Unsupported operation. Throws UnsupportedOperationException if invoked. Druid filters are being split from the
     * ApiRequest model and should be handled separately.
     *
     * @throws UnsupportedOperationException if invoked.
     */
    @Deprecated
    @Override
    public Map<Dimension, Set<ApiFilter>> generateFilters(
            final String filterQuery, final LogicalTable table, final DimensionDictionary dimensionDictionary
    ) {
        throw new UnsupportedOperationException("Druid filters are being split from the ApiRequest model and " +
                "should be handled separately.");
    }

    /**
     * Unsupported operation. Throws {@link UnsupportedOperationException} if invoked. Druid specific logic is being
     * removed from the api request model.
     *
     * @throws UnsupportedOperationException if invoked.
     */
    @Deprecated
    @Override
    public DataApiRequest withBuilder(Response.ResponseBuilder builder) {
        throw new UnsupportedOperationException("Druid specific logic is being removed from the api request model");
    }

    /**
     * Unsupported operation. Throws {@link UnsupportedOperationException} if invoked. Druid specific logic is being
     * removed from the api request model.
     *
     * @throws UnsupportedOperationException if invoked.
     */
    @Deprecated
    @Override
    public DataApiRequest withFilterBuilder(final DruidFilterBuilder filterBuilder) {
        throw new UnsupportedOperationException("Druid specific logic is being removed from the api request model");
    }
}
