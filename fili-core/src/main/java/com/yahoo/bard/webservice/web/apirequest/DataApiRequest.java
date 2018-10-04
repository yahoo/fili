// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.filterbuilders.DruidFilterBuilder;
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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import javax.ws.rs.core.Response;

/**
 * DataApiRequest Request binds, validates, and models the parts of a request to the data endpoint.
 */
public interface DataApiRequest extends ApiRequest {
    String REQUEST_MAPPER_NAMESPACE = "dataApiRequestMapper";
    String RATIO_METRIC_CATEGORY = "Ratios";
    String DATE_TIME_STRING = "dateTime";

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
     * The list of sorting predicates for this query.
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
    OptionalInt getTopN();

    /**
     * An optional limit of records returned.
     *
     * @return An optional integer.
     */
    OptionalInt getCount();

    // Query model objects

    /**
     * Builds and returns the Druid filters from this request's {@link ApiFilter}s.
     * <p>
     * The Druid filters are built (an expensive operation) every time this method is called. Use it judiciously.
     *
     * @return the Druid filter
     */
    Filter getDruidFilter();

    /**
     *  The fact model having (should probably remove this).
     *
     *  @return A fact model having
     */
    Having getDruidHaving();

    // Builder methods

     /**
     * Generates filter objects on the based on the filter query in the api request.
     *
     * @param filterQuery  Expects a URL filter query String in the format:
     * (dimension name).(fieldname)-(operation):[?(value or comma separated values)]?
     * @param table  The logical table for the data request
     * @param dimensionDictionary  DimensionDictionary
     *
     * @return Set of filter objects.
     * @deprecated Use FilterBinders.INSTANCE::generateFilters as an alternative
     */
    @Deprecated
    Map<Dimension, Set<ApiFilter>> generateFilters(
            String filterQuery,
            LogicalTable table,
            DimensionDictionary dimensionDictionary
    );
    /**
     *  Get the filter builder tool for downstream filter building.
     *
     * @return A favtory for building filters.
     *
     * @deprecated Current client should provide their own filter builders.
     */
    @Deprecated
    DruidFilterBuilder getFilterBuilder();

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
     * @deprecated Use @see{{@link #withIntervals(List)}}
     */
    @Deprecated
    DataApiRequest withIntervals(Set<Interval> intervals);

    DataApiRequest withFilters(ApiFilters filters);

    DataApiRequest withHavings(Map<LogicalMetric, Set<ApiHaving>> havings);

    DataApiRequest withSorts(LinkedHashSet<OrderByColumn> sorts);

    DataApiRequest withTimeSort(Optional<OrderByColumn> timeSort);

    DataApiRequest withTimeZone(DateTimeZone timeZone);

    // Result Set Truncations

    DataApiRequest withTopN(int topN);

    DataApiRequest withCount(int count);

    DataApiRequest withPaginationParameters(Optional<PaginationParameters> paginationParameters);

    // Presentation

    DataApiRequest withFormat(ResponseFormatType format);

    // Processing concerns

    DataApiRequest withAsyncAfter(long asyncAfter);

    // Binder methods

    DataApiRequest withBuilder(Response.ResponseBuilder builder);

    DataApiRequest withFilterBuilder(DruidFilterBuilder filterBuilder);

    DruidFilterBuilder getDruidFilterBuilder();

    // CHECKSTYLE:ON
}
