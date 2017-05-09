// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import static com.yahoo.bard.webservice.druid.model.query.QueryContext.Param.BY_SEGMENT;
import static com.yahoo.bard.webservice.druid.model.query.QueryContext.Param.FINALIZE;
import static com.yahoo.bard.webservice.druid.model.query.QueryContext.Param.POPULATE_CACHE;
import static com.yahoo.bard.webservice.druid.model.query.QueryContext.Param.PRIORITY;
import static com.yahoo.bard.webservice.druid.model.query.QueryContext.Param.QUERY_ID;
import static com.yahoo.bard.webservice.druid.model.query.QueryContext.Param.TIMEOUT;
import static com.yahoo.bard.webservice.druid.model.query.QueryContext.Param.UNCOVERED_INTERVALS_LIMIT;
import static com.yahoo.bard.webservice.druid.model.query.QueryContext.Param.USE_CACHE;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableMap;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * QueryContext.
 */
public class QueryContext {
    /**
     * Enumeration of the available query context keys that can be set.
     */
    public enum Param {
        TIMEOUT("timeout"),
        PRIORITY("priority"),
        QUERY_ID("queryId"),
        USE_CACHE("useCache"),
        POPULATE_CACHE("populateCache"),
        BY_SEGMENT("bySegment"),
        FINALIZE("finalize"),
        UNCOVERED_INTERVALS_LIMIT("uncoveredIntervalsLimit")
        ;

        private final String jsonName;

        /**
         * Constructor.
         *
         * @param jsonName  Name of the parameter
         */
        Param(String jsonName) {
            this.jsonName = jsonName;
        }

        String getName() {
            return jsonName;
        }
    }

    // ACCEPTING_FIELDS holds the list of legitimate parameters to context and their expected types
    @SuppressWarnings("rawtypes")
    public static final Map<Param, Class> ACCEPTING_FIELDS = ImmutableMap.copyOf(Stream.of(
            new SimpleImmutableEntry<>(TIMEOUT, Number.class),
            new SimpleImmutableEntry<>(PRIORITY, Number.class),
            new SimpleImmutableEntry<>(QUERY_ID, String.class),
            new SimpleImmutableEntry<>(USE_CACHE, Boolean.class),
            new SimpleImmutableEntry<>(POPULATE_CACHE, Boolean.class),
            new SimpleImmutableEntry<>(BY_SEGMENT, Boolean.class),
            new SimpleImmutableEntry<>(FINALIZE, Boolean.class),
            new SimpleImmutableEntry<>(UNCOVERED_INTERVALS_LIMIT, Number.class))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    @JsonIgnore
    public final Map<Param, Object> contextMap;

    @JsonIgnore
    protected final AtomicLong totalQueries;

    @JsonIgnore
    protected final long sequenceNumber;

    /**
     * Constructor.
     *
     * @param contextMap  Map of context values
     * @param totalQueries  Number of queries that this context is attached to
     */
    public QueryContext(Map<Param, Object> contextMap, AtomicLong totalQueries) {
        // Ensure that all contained context fields are of the correct type
        for (Map.Entry<Param, Object> entry: contextMap.entrySet()) {
            validateField(entry.getKey(), entry.getValue());
        }

        // Store an immutable copy
        this.contextMap = ImmutableMap.copyOf(contextMap);
        this.totalQueries = totalQueries == null ? new AtomicLong(0) : totalQueries;
        this.sequenceNumber = this.totalQueries.incrementAndGet();
    }

    /**
     * Constructor.
     *
     * @param contextMap  Map of context values
     */
    public QueryContext(Map<Param, Object> contextMap) {
        this(contextMap, null);
    }

    /**
     * Constructor.
     *
     * @param copy  QueryContext to gather sequence number and total queries from
     * @param contextMap  Map of context values to override with
     */
    protected QueryContext(QueryContext copy, Map<Param, Object> contextMap) {
        // Ensure that all contained context fields are of the correct type
        for (Map.Entry<Param, Object> entry: contextMap.entrySet()) {
            validateField(entry.getKey(), entry.getValue());
        }

        // Store an immutable copy
        this.contextMap = ImmutableMap.copyOf(contextMap);
        this.totalQueries = copy.totalQueries;
        this.sequenceNumber = copy.sequenceNumber;
    }

    /**
     * Validate that the given field and value are allowed in the context.
     *
     * @param param  Field to evaluate
     * @param value  Value to evaluate
     */
    @SuppressWarnings("rawtypes")
    public static void validateField(Param param, Object value) {
        // Any null value is fine for fields we know about
        if (value == null) {
            return;
        }

        // Make sure the type is what we expect it to be
        Class acceptingClass = ACCEPTING_FIELDS.get(param);
        if (!acceptingClass.isInstance(value)) {
            String message = String.format(
                    "%s expects type %s but found %s",
                    param.getName(),
                    acceptingClass.getName(),
                    value.getClass().getName()
            );
            throw new IllegalArgumentException(message);
        }
    }

    @JsonIgnore
    public boolean isEmpty() {
        return contextMap.size() == 0;
    }

    /**
     * Fork the QueryContext, cloning the values and continuing the number fo queries.
     *
     * @return the forked context
     */
    @JsonIgnore
    public QueryContext fork() {
        return new QueryContext(new HashMap<>(contextMap), this.totalQueries);
    }

    /**
     * Copy-mutate the context on the given field with the given value.
     *
     * @param param  Field to set in the clone
     * @param value  Value to set in the clone
     *
     * @return a clone of this context with the new value set
     */
    protected QueryContext withValue(Param param, Object value) {
        validateField(param, value);
        Map<Param, Object> values = new HashMap<>(contextMap);
        if (value == null) {
            values.remove(param);
        } else {
            values.put(param, value);
        }
        return new QueryContext(this, values);
    }

    // CHECKSTYLE:OFF
    public QueryContext withTimeout(Integer timeout) {
        return withValue(TIMEOUT, timeout);
    }

    public QueryContext withPriority(Integer priority) {
        return withValue(PRIORITY, priority);
    }

    public QueryContext withQueryId(String queryId) {
        return withValue(QUERY_ID, queryId);
    }

    public QueryContext withUseCache(Boolean useCache) {
        return withValue(USE_CACHE, useCache);
    }

    public QueryContext withPopulateCache(Boolean populateCache) {
        return withValue(POPULATE_CACHE, populateCache);
    }

    public QueryContext withBySegment(Boolean bySegment) {
        return withValue(BY_SEGMENT, bySegment);
    }

    public QueryContext withFinalize(Boolean finalize) {
        return withValue(FINALIZE, finalize);
    }

    public QueryContext withUncoveredIntervalsLimit(Integer uncoveredIntervalsLimit) {
        return withValue(UNCOVERED_INTERVALS_LIMIT, uncoveredIntervalsLimit);
    }
    // CHECKSTYLE:ON

    @JsonIgnore
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @JsonIgnore
    public long getNumberOfQueries() {
        return totalQueries.get();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getTimeout() {
        return (Integer) contextMap.get(TIMEOUT);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getPriority() {
        return (Integer) contextMap.get(PRIORITY);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getQueryId() {
        return (String) contextMap.get(QUERY_ID) + "_" + sequenceNumber;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Boolean getUseCache() {
        return (Boolean) contextMap.get(USE_CACHE);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Boolean getPopulateCache() {
        return (Boolean) contextMap.get(POPULATE_CACHE);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Boolean getBySegment() {
        return (Boolean) contextMap.get(BY_SEGMENT);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Boolean getFinalize() {
        return (Boolean) contextMap.get(FINALIZE);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getUncoveredIntervalsLimit() {
        return (Integer) contextMap.get(UNCOVERED_INTERVALS_LIMIT);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryContext) {
            QueryContext that = (QueryContext) o;
            return this.contextMap.equals(that.contextMap);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(contextMap);
    }
}
