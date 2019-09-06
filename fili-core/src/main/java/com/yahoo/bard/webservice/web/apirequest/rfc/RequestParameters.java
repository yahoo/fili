package com.yahoo.bard.webservice.web.apirequest.rfc;

import java.util.List;
import java.util.Optional;

import javax.ws.rs.core.PathSegment;

public class RequestParameters {

    /**
     * TODO document ALL of these parameters. What they are, the form they are expected in, etc.
     */
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

    public Optional<String> getLogicalTable() {
        return logicalTable;
    }

    public Optional<String> getGranularity() {
        return granularity;
    }

    public List<PathSegment> getDimensions() {
        return dimensions;
    }

    public Optional<String> getLogicalMetrics() {
        return logicalMetrics;
    }

    public Optional<String> getIntervals() {
        return intervals;
    }

    public Optional<String> getFilters() {
        return apiFilters;
    }

    public Optional<String> getHavings() {
        return havings;
    }

    public Optional<String> getSorts() {
        return sorts;
    }

    public Optional<String> topN() {
        return topN;
    }

    public Optional<String> getCount() {
        return count;
    }

    public Optional<String> getFormat() {
        return format;
    }

    public Optional<String> getDownloadFilename() {
        return downloadFilename;
    }

    public Optional<String> getTimeZone() {
        return timeZone;
    }

    public Optional<String> getAsyncAfter() {
        return asyncAfter;
    }

    public Optional<String> getPerPage() {
        return perPage;
    }

    public Optional<String> getPage() {
        return page;
    }
}
