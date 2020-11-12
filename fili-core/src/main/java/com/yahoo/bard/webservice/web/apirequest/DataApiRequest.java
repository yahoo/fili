// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder;
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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.ws.rs.core.Response;

/**
 * DataApiRequest Request binds, validates, and models the parts of a request to the data endpoint.
 */
public interface DataApiRequest extends ApiRequest {

    String REQUEST_MAPPER_NAMESPACE = "dataApiRequestMapper";
    String METRIC_GENERATOR_NAMESPACE = "metric_generator";
    String ORDER_BY_GENERATOR_NAMESPACE = "order_by_generator";


    String RATIO_METRIC_CATEGORY = "Ratios";
    String DATE_TIME_STRING = "dateTime";

    // utility methods

    /**
     * Utility method which extracts the date time for from the provided set of sorts.
     *
     * @param sorts  The sorts to extract the date time sort from
     * @return an optional containing the date time sort if one was present in the set of sorts.
     */
    static Optional<OrderByColumn> extractDateTimeSort(LinkedHashSet<OrderByColumn> sorts) {
        return sorts.stream()
                .filter(orderBy -> orderBy.getDimension().equalsIgnoreCase(DataApiRequest.DATE_TIME_STRING))
                .findFirst();
    }

    /**
     * Utility method which removes the date time sort from a set of sorts if a date time sort is present.
     *
     * @param sorts  The sorts to remove the date time sort from.
     * @return a copy of the input sorts without any date time sorts.
     */
    static LinkedHashSet<OrderByColumn> extractStandardSorts(LinkedHashSet<OrderByColumn> sorts) {
        return sorts.stream()
                .filter(orderBy ->
                        !orderBy.getDimension().equalsIgnoreCase(DataApiRequest.DATE_TIME_STRING))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * If present inserts a date time sort into the beginning of the provided set of sorts.
     *
     * @param dateTimeSort  An optional which may contain a date time sort to insert.
     * @param standardSorts  The set of sorts to be inserting into.
     * @return the combined sorts.
     */
    static LinkedHashSet<OrderByColumn> combineSorts(
            OrderByColumn dateTimeSort,
            LinkedHashSet<OrderByColumn> standardSorts
    ) {
        LinkedHashSet<OrderByColumn> result = new LinkedHashSet<>();
        if (dateTimeSort != null) {
            result.add(dateTimeSort);
        }
        result.addAll(standardSorts);
        return result;
    }


    // Schema fields

    /**
     * The logical table for this request.
     *
     * @return A logical table
     */
    LogicalTable getTable();

    /**
     * The grain to group the results of this request.
     *
     * @return a granularity
     */
    Granularity getGranularity();

    /**
     * The set of grouping dimensions on this ApiRequest.
     *
     * @return a set of dimensions
     */
    Set<Dimension> getDimensions();

    /**
     * A map of dimension fields specified for the output schema.
     *
     * @return The dimension fields for output grouped by their dimension
     */
    LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> getDimensionFields();

    /**
     * The logical metrics requested in this query.
     *
     * @return A set of logical metrics
     */
    Set<LogicalMetric> getLogicalMetrics();

    // Aggregation constraints

    /**
     * The interval constraints for this query.
     *
     * @return A list of intervals
     */
    List<Interval> getIntervals();

    /**
     * The filters for this ApiRequest, grouped by dimensions.
     *
     * @return a map of filters by dimension
     */
    ApiFilters getApiFilters();

    /**
     * Get the dimensions used in filters on this request.
     *
     * @return A set of dimensions
     *
     * @deprecated Use {@link #getApiFilters()} keyset
     */
    @Deprecated
    default Set<Dimension> getFilterDimensions() {
        return getApiFilters().keySet();
    }

    /**
     * The api having constraints for this request, grouped by logical metrics.
     *
     * @return a map of havings by metrics.
     */
    Map<LogicalMetric, Set<ApiHaving>> getHavings();

    // Row sequence constraints

    /**
     * The list of sorting predicates for this query. These sorts do NOT include the time sort.
     *
     * @return The sorting columns
     */
    LinkedHashSet<OrderByColumn> getSorts();

    /**
     * An optional sorting predicate for the time column.
     *
     * @return The sort direction
     */
    Optional<OrderByColumn> getDateTimeSort();

    /**
     * Returns the combined set of standard sorts and date time sort.
     *
     * @return all sorts in the request
     */
    default LinkedHashSet<OrderByColumn> getAllSorts() {
        LinkedHashSet<OrderByColumn> allSorts = new LinkedHashSet<>();
        getDateTimeSort().ifPresent(allSorts::add);
        allSorts.addAll(getSorts());
        return allSorts;
    }

    /**
     * The date time zone to apply to the dateTime parameter and to express the response and granularity in.
     *
     * @return A time zone
     */
    DateTimeZone getTimeZone();

    // Result Set Truncations

    /**
     * The limit per time bucket for a top n query.
     *
     * @return The number of values per bucket.
     */
    Optional<Integer> getTopN();

    /**
     * An optional limit of records returned.
     *
     * @return An optional integer.
     */
    Optional<Integer> getCount();

    // Query model objects

    /**
     * Builds and returns the Druid filters from this request's {@link ApiFilter}s.
     * <p>
     * The Druid filters are built (an expensive operation) every time this method is called. Use it judiciously.
     *
     * @return the filter used in the Query Model
     *
     * @deprecated Build query filters outside the API Request using FilterBuilders
     */
    @Deprecated
    Filter getQueryFilter();

    /**
     * Builds and returns the Druid filters from this request's {@link ApiFilter}s.
     * <p>
     * The Druid filters are built (an expensive operation) every time this method is called. Use it judiciously.
     *
     * @return the Druid filter
     */
    @Deprecated
    default Filter getDruidFilter() {
        return getQueryFilter();
    }

    /**
     * The fact model having (should probably remove this).
     *
     * @return A fact model having
     *
     * @deprecated Query builders should have responsibility for building Having
     */
    @Deprecated
    Having getQueryHaving();

    /**
     *  The fact model having (should probably remove this).
     *
     * @return A fact model having
     *
     * @deprecated Use {@link #getQueryHaving()}
     */
    @Deprecated
    default Having getHaving() {
        return getQueryHaving();
    }

    // Builder methods

    /**
     *  Get the filter builder tool for downstream filter building.
     *
     * @return A favtory for building filters.
     *
     * @deprecated Current client should provide their own filter builders.
     */
    @Deprecated
    DruidFilterBuilder getFilterBuilder();

     /**
     * Generates filter objects on the based on the filter query in the api request.
     *
     * @param filterQuery  Expects a URL filter query String in the format:
     * (dimension name).(fieldname)-(operation):[?(value or comma separated values)]?
     * @param table  The logical table for the data request
     * @param dimensionDictionary  DimensionDictionary
     *
     * @return Set of filter objects.
     * @deprecated Use FilterBinders.getInstance()::generateFilters as an alternative
     */
    @Deprecated
    Map<Dimension, Set<ApiFilter>> generateFilters(
            String filterQuery,
            LogicalTable table,
            DimensionDictionary dimensionDictionary
    );

    // CHECKSTYLE:OFF

    // Schema fields
    DataApiRequest withTable(LogicalTable table);

    DataApiRequest withGranularity(Granularity granularity);

    DataApiRequest withDimensions(LinkedHashSet<Dimension> dimensions);

    DataApiRequest withPerDimensionFields(
            LinkedHashMap<Dimension,
                    LinkedHashSet<DimensionField>> perDimensionFields
    );

    DataApiRequest withLogicalMetrics(LinkedHashSet<LogicalMetric> logicalMetrics);

    DataApiRequest withIntervals(List<Interval> intervals);

    /**
     * @param intervals  The intervals being applied.
     *
     * @return a new DataApiRequest
     *
     * @deprecated Use @see{{@link #withIntervals(List)}}
     */
    @Deprecated
    DataApiRequest withIntervals(Set<Interval> intervals);

    DataApiRequest withFilters(ApiFilters filters);

    @Deprecated
    default DataApiRequest withHavings(Map<LogicalMetric, Set<ApiHaving>> havings) {
        return withHavings(new LinkedHashMap<>(havings));
    }

    default DataApiRequest withHavings(LinkedHashMap<LogicalMetric, Set<ApiHaving>> havings) {
        throw new UnsupportedOperationException("this method has not been implemented");
    }

    /**
     * Replaces the existing date time sort and standard sorts with sorts from this set.
     *
     * @param allSorts  A set of sorts that can contain both standard sorts and a single date time sort.
     * @return  a copy of DataApiRequest with the sorts replaced.
     */
    default DataApiRequest withAllSorts(LinkedHashSet<OrderByColumn> allSorts) {
        Optional<OrderByColumn> dateTimeSort = extractDateTimeSort(allSorts);
        LinkedHashSet<OrderByColumn> standardSorts = extractStandardSorts(allSorts);
        DataApiRequest withDateTimeSort = withTimeSort(dateTimeSort);
        return withDateTimeSort.withSorts(standardSorts);
    }

    /**
     * Replaces the existing standard sorts the provided sorts.
     *
     * @param sorts the set of sorts to replace the current standard sorts
     * @return  a copy of DataApiRequest with the standard sorts replaced but the date time sort remaining untouched
     */
    DataApiRequest withSorts(LinkedHashSet<OrderByColumn> sorts);

    /**
     * Replaces the existing date time sort with the provided sort.
     *
     * @param timeSort the sort to replace the current date time sort
     * @return  a copy of DataApiRequest with the date time sort replaced and the standard sorts remaining untouched
     */
    DataApiRequest withTimeSort(OrderByColumn timeSort);

    /**
     * Replaces the existing date time sort with the provided sort.
     *
     * @param timeSort the sort to replace the current date time sort
     * @return  a copy of DataApiRequest with the date time sort replaced and the standard sorts remaining untouched
     */
    @Deprecated
    default DataApiRequest withTimeSort(Optional<OrderByColumn> timeSort) {
        return withTimeSort(timeSort.orElse(null));
    }

    DataApiRequest withTimeZone(DateTimeZone timeZone);

    // Result Set Truncations

    DataApiRequest withTopN(Integer topN);

    DataApiRequest withCount(Integer count);

    @Deprecated
    default DataApiRequest withPaginationParameters(Optional<PaginationParameters> paginationParameters) {
        return withPaginationParameters(paginationParameters.orElse(null));
    };

    DataApiRequest withPaginationParameters(PaginationParameters paginationParameters);

    // Presentation

    DataApiRequest withFormat(ResponseFormatType format);

    DataApiRequest withDownloadFilename(String downloadFilename);

    // Processing concerns

    DataApiRequest withAsyncAfter(long asyncAfter);

    /**
     * Whether or not to attempt to build an optimal backend query (i.e. topN or timeSeries in the case of Druid) if possible.
     *
     * @return True if the backend query built from this request can be safely optimized (i.e. converted into a topN
     * or timeseries query when hitting Druid), false if a naive query should be built (i.e. groupBy in the
     * case of Druid)
     */
    default boolean optimizeBackendQuery() {
        return true;
    }

    // Builder with methods

    @Deprecated
    DataApiRequest withBuilder(Response.ResponseBuilder builder);

    @Deprecated
    DataApiRequest withFilterBuilder(DruidFilterBuilder filterBuilder);

    /**
     * The set of referenced dimensions on this ApiRequest.
     *
     * @return a set of dimensions
     */
    default Set<Dimension> getAllGroupingDimensions() {
        return Stream.of(
                getDimensions(),
                getLogicalMetrics().stream()
                        .map(LogicalMetric::getTemplateDruidQuery)
                        .filter(Objects::nonNull)
                        .map(TemplateDruidQuery::getDimensions)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet())
        ).flatMap(Set::stream).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    // CHECKSTYLE:ON
}
