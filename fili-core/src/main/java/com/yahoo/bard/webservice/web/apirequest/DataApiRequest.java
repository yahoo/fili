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

    /**
     * Builds and returns the Druid filters from this request's {@link ApiFilter}s.
     * <p>
     * The Druid filters are built (an expensive operation) every time this method is called. Use it judiciously.
     *
     * @return the Druid filter
     */
     Filter getDruidFilter();

    /**
     * The having constraints for this request, grouped by logical metrics.
     *
     * @return a map of havings by metrics.
     */
     Map<LogicalMetric, Set<ApiHaving>> getHavings();

    /**
     *  The fact model having (should probably remove this).
     *
     *  @return A fact model having
     */
     Having getHaving();

    /**
     * A prioritized list of sort columns.
     *
     * @return sort columns.
     */
     LinkedHashSet<OrderByColumn> getSorts();

    /**
     * An optional limit of records returned.
     *
     * @return An optional integer.
     */
     OptionalInt getCount();

    /**
     * The limit per time bucket for a top n query.
     *
     * @return The number of values per bucket.
     */
     OptionalInt getTopN();

    /**
     * The date time zone to apply to the dateTime parameter and to express the response and granularity in.
     *
     * @return A time zone
     */
     DateTimeZone getTimeZone();

    /**
     * A filter builder (remove).
     *
     * @return a filter builder.
     */
     DruidFilterBuilder getFilterBuilder();

    /**
     * An optional sorting predicate for the time column.
     *
     * @return The sort direction
     */
     Optional<OrderByColumn> getDateTimeSort();

    /**
     * Get the dimensions used in filters on this request.
     *
     * @return A set of dimensions
     */
     Set<Dimension> getFilterDimensions();

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
     * @return  The dimension fields for output grouped by their dimension
     */
     LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> getDimensionFields();

    /**
     * The logical metrics requested in this query.
     *
     * @return A set of logical metrics
     */
     Set<LogicalMetric> getLogicalMetrics();

    /**
     * The intervals for this query.
     *
     * @return A set of intervals
     */
     List<Interval> getIntervals();

    /**
     * The filters for this ApiRequest, grouped by dimensions.
     *
     * @return a map of filters by dimension
     */
     ApiFilters getApiFilters();

    /**
     * Generates filter objects on the based on the filter query in the api request.
     *
     * @param filterQuery  Expects a URL filter query String in the format:
     * (dimension name).(fieldname)-(operation):[?(value or comma separated values)]?
     * @param table  The logical table for the data request
     * @param dimensionDictionary  DimensionDictionary
     *
     * @return Set of filter objects.
     */
    Map<Dimension, Set<ApiFilter>> generateFilters(
            String filterQuery,
            LogicalTable table,
            DimensionDictionary dimensionDictionary
    );

    // CHECKSTYLE:OFF
    DataApiRequestImpl withFormat(ResponseFormatType format);

    DataApiRequestImpl withPaginationParameters(Optional<PaginationParameters> paginationParameters);

    DataApiRequestImpl withBuilder(Response.ResponseBuilder builder);

    DataApiRequestImpl withTable(LogicalTable table);

    DataApiRequestImpl withGranularity(Granularity granularity);

    DataApiRequestImpl withDimensions(LinkedHashSet<Dimension> dimensions);

    DataApiRequestImpl withPerDimensionFields(LinkedHashMap<Dimension,
            LinkedHashSet<DimensionField>> perDimensionFields);

    DataApiRequestImpl withLogicalMetrics(LinkedHashSet<LogicalMetric> logicalMetrics);

    DataApiRequestImpl withIntervals(List<Interval> intervals);

    @Deprecated
    /**
     * @deprecated Use @see{{@link #withIntervals(List)}}
     */
    DataApiRequestImpl withIntervals(Set<Interval> intervals);

    DataApiRequestImpl withFilters(ApiFilters filters);

    DataApiRequestImpl withHavings(Map<LogicalMetric, Set<ApiHaving>> havings);

    DataApiRequestImpl withHaving(Having having);

    DataApiRequestImpl withSorts(LinkedHashSet<OrderByColumn> sorts);

    DataApiRequestImpl withCount(int count);

    DataApiRequestImpl withTopN(int topN);

    DataApiRequestImpl withAsyncAfter(long asyncAfter);

    DataApiRequestImpl withTimeZone(DateTimeZone timeZone);

    DataApiRequestImpl withFilterBuilder(DruidFilterBuilder filterBuilder);
}
