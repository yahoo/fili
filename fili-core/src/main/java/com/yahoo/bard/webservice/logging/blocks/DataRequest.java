// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.logging.LogInfo;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.ApiFilter;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Corresponds mainly to the requesting part of a request served by the DataServlet.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class DataRequest implements LogInfo {
    protected final String resource = "data";
    protected final String table;
    protected final String timeGrain;
    protected final List<Interval> intervals;
    protected final int numBuckets;

    protected final List<Filter> filters;
    protected final List<String> metrics;
    protected final List<String> groupByDimensions;
    protected final Set<String> combinedDimensions;
    protected final Set<String> dataSources;
    protected final boolean skipCache;
    protected final String format;

    /**
     * Constructor.
     *
     * @param table  Which logical table the request was for
     * @param intervals  Over what intervals the request was for
     * @param filterSuperSet  Collection of all of the filters in the API request
     * @param metricSet  All of the metrics in the API request
     * @param groupByDimensionsSet  Dimensions grouped on in the API request
     * @param dataSourceNames  Names of the data sources selected for the queries
     * @param readCache  Indicate if the user turned off cached responses
     * @param format  In which format the request asked for a response
     */
    public DataRequest(
            LogicalTable table,
            List<Interval> intervals,
            Collection<Set<ApiFilter>> filterSuperSet,
            Set<LogicalMetric> metricSet,
            Set<Dimension> groupByDimensionsSet,
            Set<String> dataSourceNames,
            boolean readCache,
            String format
    ) {
        this.table = table.getName();
        this.timeGrain = table.getGranularity().toString();
        this.numBuckets = 0;

        this.intervals = intervals;

        this.filters = new ArrayList<>();
        this.combinedDimensions = new TreeSet<>();
        for (Set<ApiFilter> filterSet : filterSuperSet) {
            for (ApiFilter apiFilter : filterSet) {
                Filter filter = new Filter(apiFilter);
                this.filters.add(filter);
                this.combinedDimensions.add(filter.dimension);
            }
        }
        Collections.sort(this.filters);

        this.metrics = metricSet.stream().map(LogicalMetric::getName).sorted().collect(Collectors.toList());

        this.groupByDimensions = groupByDimensionsSet
                .stream()
                .map(Dimension::getApiName)
                .sorted()
                .collect(Collectors.toList());

        this.combinedDimensions.addAll(this.groupByDimensions);

        this.dataSources = dataSourceNames;
        this.skipCache = !readCache;
        this.format = format;
    }
}
