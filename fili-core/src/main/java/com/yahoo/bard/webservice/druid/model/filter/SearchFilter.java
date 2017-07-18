// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Filter for a search.
 * <code>
 * {
 *  "type": "search",
 *  "dimension": "Dim",
 *  "query": {
 *      "type": "insensitive_contains",
 *      "value": "Val"
 * }
 * </code>
 *
 */
public class SearchFilter extends DimensionalFilter {

    /**
     * Query type for the search.
     */
    public enum QueryType {
        InsensitiveContains("insensitive_contains"),
        Fragment("fragment"),
        Contains("contains");

        String type;

        /**
         * Constructor.
         *
         * @param type  Type of the query type (for serialization)
         */
        QueryType(String type) {
            this.type = type;
        }

        /**
         * Get the QueryType enum from it's search type.
         *
         * @param type  The type of query (i.e. "insensitive_contains")
         *
         * @return the enum QueryType if found otherwise empty.
         */
        public static Optional<QueryType> fromType(String type) {
            for (QueryType queryType : values()) {
                if (queryType.type.equals(type)) {
                    return Optional.of(queryType);
                }
            }
            return Optional.empty();
        }
    }

    private final Map<String, String> query;

    /**
     * Constructor.
     *
     * @param dimension  Dimension to search
     * @param queryType  Type of search to run
     * @param value  Value to search for
     */
    public SearchFilter(Dimension dimension, QueryType queryType, String value) {
        this(dimension, queryType.type, value);
    }

    /**
     * Constructor.
     *
     * @param dimension  Dimension to search
     * @param type  Type of search to run
     * @param value  Value to search for
     */
    private SearchFilter(Dimension dimension, String type, String value) {
        super(dimension, DefaultFilterType.SEARCH);
        this.query = Collections.unmodifiableMap(new LinkedHashMap<String, String>() {
            {
                put("type", type);
                put("value", value);
            }
        });
    }

    public Map<String, String> getQuery() {
        return query;
    }

    private String getQueryType() {
        return query.get("type");
    }

    private String getQueryValue() {
        return query.get("value");
    }

    @Override
    public SearchFilter withDimension(Dimension dimension) {
        return new SearchFilter(dimension, getQueryType(), getQueryValue());
    }

    // CHECKSTYLE:OFF
    public SearchFilter withQueryType(QueryType queryType) {
        return new SearchFilter(getDimension(), queryType.type, getQueryValue());
    }

    public SearchFilter withValue(String value) {
        return new SearchFilter(getDimension(), getQueryType(), value);
    }
    // CHECKSTYLE:ON

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), query);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        SearchFilter other = (SearchFilter) obj;
        return
                super.equals(obj) &&
                Objects.equals(query, other.query);
    }
}
