// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.web.apirequest.generator.Generator;

import java.util.List;
import java.util.Optional;

import javax.ws.rs.core.PathSegment;

/**
 * Data object all raw pieces of data in an api request.
 */
public class RequestParameters {

    private final Optional<String> logicalTable;
    private final Optional<String> granularity;
    private final List<PathSegment> dimensions;
    private final Optional<String> logicalMetrics;
    private final Optional<String> intervals;
    private final Optional<String> apiFilters;
    private final Optional<String> havings;
    private final Optional<String> sorts;
    private final Optional<String> count;
    private final Optional<String> topN;
    private final Optional<String> format;
    private final Optional<String> downloadFilename;
    private final Optional<String> timeZone;
    private final Optional<String> asyncAfter;
    private final Optional<String> perPage;
    private final Optional<String> page;

    /**
     * Constructor. All parameters are unparsed at this point and likely require extra processing, likely by a
     * {@link Generator} implementation.
     *
     * @param logicalTable  The name of the logical table for this query to run against.
     * @param granularity  The granularity of the query (e.g. day, month, etc.)
     * @param dimensions  The grouping dimensions and the set of requested fields for each. The dimension api name is
     *                    the path element, and the fields should be in the multimap, in the form
     *                    {@code show => fieldNames, ...} if requested fields are present.
     * @param logicalMetrics  The names of the desired metrics.
     * @param intervals  The interval the query should go over (e.g. 2019-01-01/2019-01-02)
     * @param apiFilters  The set of filters for this query
     * @param havings  The set of havings (filters on metrics) for this query
     * @param sorts  The sorts for this query (e.g. sort on revenue descending)
     * @param count  The the count of the query (????? what does count do)
     * @param topN  The limit on the sort (e.g. sort by revenue descending, give me top 3 results)
     * @param format  The format of the response should be in
     * @param downloadFilename  The filename of the response. The presence of this parameter indicated the result
     *                          should be returned as a file to be downloaded by the client.
     * @param timeZone  The time zone this query should use. Affects aggregation and time macros in intervals
     * @param asyncAfter  If the query takes longer that this time the result should be prepared as a file
     *                    asynchronously, and a link to download the file when it is ready should be provided to the
     *                    client.
     * @param perPage  Pagination parameter, how many results per page.
     * @param page  Pagination parameter, which page of the result to get.
     */
    public RequestParameters(
            String logicalTable,
            String granularity,
            List<PathSegment> dimensions,
            String logicalMetrics,
            String intervals,
            String apiFilters,
            String havings,
            String sorts,
            String count,
            String topN,
            String format,
            String downloadFilename,
            String timeZone,
            String asyncAfter,
            String perPage,
            String page
    ) {
        // this class is a container for druid request parameters. We do no internal calculations using these
        // parameters, so it is fine to store them as Optional to avoid allocating an Optional on every get.
        this.logicalTable = Optional.ofNullable(logicalTable);
        this.granularity = Optional.ofNullable(granularity);
        this.dimensions = dimensions;
        this.logicalMetrics = Optional.ofNullable(logicalMetrics);
        this.intervals = Optional.ofNullable(intervals);
        this.apiFilters = Optional.ofNullable(apiFilters);
        this.havings = Optional.ofNullable(havings);
        this.sorts = Optional.ofNullable(sorts);
        this.count = Optional.ofNullable(count);
        this.topN = Optional.ofNullable(topN);
        this.format = Optional.ofNullable(format);
        this.downloadFilename = Optional.ofNullable(downloadFilename);
        this.timeZone = Optional.ofNullable(timeZone);
        this.asyncAfter = Optional.ofNullable(asyncAfter);
        this.perPage = Optional.ofNullable(perPage);
        this.page = Optional.ofNullable(page);
    }

    /**
     * Getter for the logical table.
     *
     * @return the logical table
     */
    public Optional<String> getLogicalTable() {
        return logicalTable;
    }

    /**
     * Getter for the granularity.
     *
     * @return the granularity
     */
    public Optional<String> getGranularity() {
        return granularity;
    }

    /**
     * Getter for the list of dimensions and their requested fields.
     *
     * @return the dimensions
     */
    public List<PathSegment> getDimensions() {
        return dimensions;
    }

    /**
     * Getter for the logical metrics.
     *
     * @return the logical metrics
     */
    public Optional<String> getLogicalMetrics() {
        return logicalMetrics;
    }

    /**
     * Getter for the intervals.
     *
     * @return the intervals
     */
    public Optional<String> getIntervals() {
        return intervals;
    }

    /**
     * Getter for the filters.
     *
     * @return the filters
     */
    public Optional<String> getFilters() {
        return apiFilters;
    }

    /**
     * Getter for the havings.
     *
     * @return the havings
     */
    public Optional<String> getHavings() {
        return havings;
    }

    /**
     * Getter for the sorts.
     *
     * @return the sorts
     */
    public Optional<String> getSorts() {
        return sorts;
    }

    /**
     * Getter for topN.
     *
     * @return topN
     */
    public Optional<String> topN() {
        return topN;
    }

    /**
     * Getter for the count.
     *
     * @return count
     */
    public Optional<String> getCount() {
        return count;
    }

    /**
     * Getter for the response format.
     *
     * @return the response format
     */
    public Optional<String> getFormat() {
        return format;
    }

    /**
     * Getter for the download filename.
     *
     * @return the download filename
     */
    public Optional<String> getDownloadFilename() {
        return downloadFilename;
    }

    /**
     * Getter for the timezone.
     *
     * @return the timezone
     */
    public Optional<String> getTimeZone() {
        return timeZone;
    }

    /**
     * Getter for async after.
     *
     * @return async after
     */
    public Optional<String> getAsyncAfter() {
        return asyncAfter;
    }

    /**
     * Getter for result per page.
     *
     * @return results per page
     */
    public Optional<String> getPerPage() {
        return perPage;
    }

    /**
     * Getter for requested page number.
     *
     * @return the page number
     */
    public Optional<String> getPage() {
        return page;
    }
}
