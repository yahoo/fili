// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.filters.ApiFilters;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.joda.time.Interval;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.ws.rs.core.Response;

/**
 * Tables API Request. Such an API Request binds, validates, and models the parts of a request to the tables endpoint.
 */
public interface TablesApiRequest extends ApiRequest {
    String REQUEST_MAPPER_NAMESPACE = "tablesApiRequestMapper";
    String EXCEPTION_HANDLER_NAMESPACE = "tablesApiRequestExceptionHandler";

    /**
     * Returns a set of LogicalTables associated with a table API request.
     *
     * @return the set of LogicalTables associated with a table API request
     */
    Set<LogicalTable> getTables();

    /**
     * Returns the LogicalTable requested in the table API request URL.
     *
     * @return the LogicalTable requested in the table API request URL
     */
    LogicalTable getTable();

    /**
     * Returns the grain to group the results of this request.
     *
     * @return the grain to group the results of this request
     */
    Granularity getGranularity();

    /**
     * Returns the set of grouping dimensions on this request.
     *
     * @return the set of grouping dimensions on this request
     */
    Set<Dimension> getDimensions();

    /**
     * Returns the dimensions used in filters on this request.
     *
     * @return the dimensions used in filters on this request
     */
    Set<Dimension> getFilterDimensions();

    /**
     * Returns a map of filters by dimension for this request, grouped by dimensions.
     *
     * @return the map of filters by dimension for this request, grouped by dimensions
     */
    ApiFilters getApiFilters();

    /**
     * Returns the intervals for this query.
     *
     * @return the intervals for this query
     */
    List<Interval> getIntervals();

    /**
     * Returns the logical metrics requested in this query.
     *
     * @return the logical metrics requested in this query
     */
    Set<LogicalMetric> getLogicalMetrics();

    // CHECKSTYLE:OFF
    TablesApiRequest withFormat(ResponseFormatType format);

    @Deprecated
    default TablesApiRequest withPaginationParameters(Optional<PaginationParameters> paginationParameters) {
        return withPaginationParameters(paginationParameters.orElse(null));
    }

    TablesApiRequest withPaginationParameters(PaginationParameters paginationParameters);

    TablesApiRequest withBuilder(Response.ResponseBuilder builder);

    TablesApiRequest withTables(LinkedHashSet<LogicalTable> tables);

    TablesApiRequest withTable(LogicalTable table);

    TablesApiRequest withGranularity(Granularity tables);

    TablesApiRequest withDimensions(LinkedHashSet<Dimension> dimensions);

    TablesApiRequest withMetrics(LinkedHashSet<LogicalMetric> metrics);

    TablesApiRequest withIntervals(List<Interval> intervals);

    TablesApiRequest withDownloadFilename(String downloadFilename);

    @Deprecated
    /**
     * @deprecated Use {@link #withIntervals(List)}
     */
    TablesApiRequest withIntervals(Set<Interval> intervals);

    TablesApiRequest withFilters(Map<Dimension, Set<ApiFilter>> filters);
    // CHECKSTYLE:ON
}
