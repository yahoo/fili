// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter;

import com.yahoo.bard.webservice.data.dimension.Dimension;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

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
         * Get the QueryType enum fromType it's search type.
         * @param type  Type of the query type (for serialization)
         * @return the enum QueryType
         */
        public static QueryType fromType(String type) {
            for (QueryType queryType : values()) {
                if (queryType.type.equals(type)) {
                    return queryType;
                }
            }
            throw new IllegalArgumentException("No query type corresponds to " + type);
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

    @Override
    public SearchFilter withDimension(Dimension dimension) {
        return new SearchFilter(dimension, query.get("type"), query.get("value"));
    }

    // CHECKSTYLE:OFF
    public SearchFilter withQueryType(QueryType queryType) {
        return new SearchFilter(getDimension(), queryType.type, query.get("value"));
    }

    public SearchFilter withValue(String value) {
        return new SearchFilter(getDimension(), query.get("type"), value);
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
