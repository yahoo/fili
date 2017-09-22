// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.util.PaginationParameters;
import org.joda.time.Interval;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Tables API Request. Such an API Request binds, validates, and models the parts of a request to the tables endpoint.
 */
public interface TablesApiRequest extends ApiRequest {
    String REQUEST_MAPPER_NAMESPACE = "tablesApiRequestMapper";

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

    // CHECKSTYLE:OFF
    TablesApiRequest withFormat(ResponseFormatType format);

    TablesApiRequest withPaginationParameters(Optional<PaginationParameters> paginationParameters);

    TablesApiRequest withUriInfo(UriInfo uriInfo);

    TablesApiRequest withBuilder(Response.ResponseBuilder builder);

    TablesApiRequest withTables(Set<LogicalTable> tables);

    TablesApiRequest withTable(LogicalTable table);

    TablesApiRequest withGranularity(Set<LogicalTable> tables);

    TablesApiRequest withTables(Granularity granularity);

    TablesApiRequest withDimensions(Set<Dimension> dimensions);

    TablesApiRequest withMetrics(Set<LogicalMetric> metrics);

    TablesApiRequest withIntervals(Set<Interval> intervals);

    TablesApiRequest withFilters(Map<Dimension, Set<ApiFilter>> filters);
    // CHECKSTYLE:ON
}
