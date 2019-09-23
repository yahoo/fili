package com.yahoo.bard.webservice.web.apirequest.generator;

import com.yahoo.bard.webservice.web.apirequest.RequestParameters;

import org.apache.commons.collections4.MultiValuedMap;
import org.glassfish.jersey.internal.util.collection.ImmutableMultivaluedMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;

public class TestRequestParameters extends RequestParameters {

    public String logicalTable;
    public String granularity;
    public List<PathSegment> dimensions;
    public String logicalMetrics;
    public String intervals;
    public String apiFilters;
    public String havings;
    public String sorts;
    public String count;
    public String topN;
    public String format;
    public String downloadFilename;
    public String timeZone;
    public String asyncAfter;
    public String perPage;
    public String page;


    public TestRequestParameters() {
        super(null, null, new ArrayList<>(), null, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    @Override public Optional<String> getLogicalTable() { return Optional.ofNullable(logicalTable); }

    @Override public Optional<String> getGranularity() { return Optional.ofNullable(granularity); }

    @Override public List<PathSegment> getDimensions() { return dimensions; }

    @Override public Optional<String> getLogicalMetrics() { return Optional.ofNullable(logicalMetrics); }

    @Override public Optional<String> getIntervals() { return Optional.ofNullable(intervals); }

    @Override public Optional<String> getFilters() { return Optional.ofNullable(apiFilters); }

    @Override public Optional<String> getHavings() { return Optional.ofNullable(havings); }

    @Override public Optional<String> getSorts() { return Optional.ofNullable(sorts); }

    @Override public Optional<String> topN() { return Optional.ofNullable(topN); }

    @Override public Optional<String> getCount() { return Optional.ofNullable(count); }

    @Override public Optional<String> getFormat() { return Optional.ofNullable(format); }

    @Override public Optional<String> getDownloadFilename() { return Optional.ofNullable(downloadFilename); }

    @Override public Optional<String> getTimeZone() { return Optional.ofNullable(timeZone); }

    @Override public Optional<String> getAsyncAfter() { return Optional.ofNullable(asyncAfter); }

    @Override public Optional<String> getPerPage() { return Optional.ofNullable(perPage); }

    @Override public Optional<String> getPage() {
        return Optional.ofNullable(page);
    }

    public static class TestPathSegment implements PathSegment {

        private final String path;
        private final MultivaluedMap<String, String> map;

        public TestPathSegment(String path) {
            this(path, new MultivaluedHashMap<String, String>());
        }

        public TestPathSegment(String path, MultivaluedMap<String, String> map) {
            this.path = path;
            this.map = new ImmutableMultivaluedMap<>(map);
        }

        @Override
        public String getPath() {
            return path;
        }

        @Override
        public MultivaluedMap<String, String> getMatrixParameters() {
            return map;
        }
    }
}
