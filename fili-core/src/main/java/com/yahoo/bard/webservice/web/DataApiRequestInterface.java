// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.filterbuilders.DruidFilterBuilder;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;

import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/**
 * A model representing an ApiRequest to the data resource.
 */
public interface DataApiRequestInterface extends ApiRequestInterface {

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
    Set<Interval> getIntervals();

    /**
     * The filters for this ApiRequest, grouped by dimensions.
     *
     * @return a map of filters by dimension
     */
    Map<Dimension, Set<ApiFilter>> getFilters();

    /**
     * Builds and returns the Druid filters from this request's {@link ApiFilter}s.
     * <p>
     * The Druid filters are built (an expensive operation) every time this method is called. Use it judiciously.
     *
     * @return the Druid filter
     */
    Filter getFilter();

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

}
